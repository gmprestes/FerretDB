// Copyright 2021 FerretDB Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package handler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"errors"

	"github.com/AlekSi/lazyerrors"
	"github.com/FerretDB/wire/wirebson"
	"github.com/google/uuid"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/FerretDB/FerretDB/v2/internal/documentdb"
	"github.com/FerretDB/FerretDB/v2/internal/handler/middleware"
	"github.com/FerretDB/FerretDB/v2/internal/handler/session"
	"github.com/FerretDB/FerretDB/v2/internal/mongoerrors"
	"github.com/FerretDB/FerretDB/v2/internal/util/must"
)

// TransactionLifetimeLimit is how long a transaction may stay open before the server rolls
// it back. It mirrors MongoDB's transactionLifetimeLimitSeconds default.
//
// The limit is not a nicety. An open transaction pins a PostgreSQL connection and holds its
// locks; a client that starts a transaction and disappears would otherwise block every
// writer of those rows until the server restarts.
const TransactionLifetimeLimit = 60 * time.Second

// txnKey identifies a transaction by the logical session that owns it.
//
// A session runs at most one transaction at a time, so the session is the whole key. The
// transaction number lives in the value and is checked against each request.
type txnKey struct {
	user      session.UserID
	sessionID uuid.UUID
}

// txn is a MongoDB transaction mapped onto a PostgreSQL one.
type txn struct {
	// busy is held while a command runs on conn.
	//
	// A pgx connection cannot be used by two goroutines at once, and the reaper runs on its
	// own: without this, it could send ROLLBACK down a connection that still has a query in
	// flight. The reaper only takes this lock opportunistically and skips the transaction
	// when a command holds it -- a busy transaction is by definition not abandoned.
	busy sync.Mutex

	// conn is pinned for the transaction's lifetime, and is nil once the transaction has
	// ended -- committed, rolled back, or discarded after a failed command.
	conn *documentdb.Conn

	number   int64
	deadline time.Time

	// aborted marks a transaction whose connection is already gone but whose session has
	// not acknowledged it yet. The entry has to outlive the connection: a driver reacts to
	// a failed command by sending abortTransaction, and it deserves a straight answer
	// instead of "never heard of it".
	aborted bool
}

// txnRegistry holds the open transactions of every session.
type txnRegistry struct {
	l  *slog.Logger
	p  *documentdb.Pool
	rw sync.Mutex
	m  map[txnKey]*txn
}

// newTxnRegistry returns an empty registry.
func newTxnRegistry(p *documentdb.Pool, l *slog.Logger) *txnRegistry {
	return &txnRegistry{
		l: l,
		p: p,
		m: map[txnKey]*txn{},
	}
}

// begin opens a transaction for the session and pins a connection to it.
func (r *txnRegistry) begin(ctx context.Context, key txnKey, number int64) (*txn, error) {
	r.rw.Lock()
	defer r.rw.Unlock()

	if existing := r.m[key]; existing != nil {
		// The session is starting a transaction while it still has one open. Whatever that
		// one was doing was never committed, so the only thing to do with it is drop it.
		r.end(ctx, key, existing, "superseded by a new transaction")
	}

	conn, err := r.p.Begin(ctx)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	t := &txn{
		conn:     conn,
		number:   number,
		deadline: time.Now().Add(TransactionLifetimeLimit),
	}
	r.m[key] = t

	r.l.DebugContext(ctx, "Transaction started", slog.Int64("txnNumber", number))

	return t, nil
}

// get returns the live transaction a command refers to.
func (r *txnRegistry) get(key txnKey, number int64) (*txn, error) {
	r.rw.Lock()
	defer r.rw.Unlock()

	t := r.m[key]
	if t == nil || t.number != number || t.aborted {
		return nil, errNoSuchTransaction()
	}

	return t, nil
}

// commit commits the session's transaction.
func (r *txnRegistry) commit(ctx context.Context, key txnKey, number int64) error {
	r.rw.Lock()
	defer r.rw.Unlock()

	t := r.m[key]
	if t == nil || t.number != number || t.aborted {
		return errNoSuchTransaction()
	}

	delete(r.m, key)

	conn := t.conn
	t.conn = nil

	if err := r.p.Commit(ctx, conn); err != nil {
		r.l.WarnContext(ctx, "Commit failed", slog.Int64("txnNumber", number), slog.Any("error", err))

		return lazyerrors.Error(err)
	}

	r.l.DebugContext(ctx, "Transaction committed", slog.Int64("txnNumber", number))

	return nil
}

// abort rolls the session's transaction back.
//
// Aborting a transaction that a failed command already rolled back succeeds: from the
// client's side the transaction did not happen either way, and answering with an error
// would only turn a handled failure into an unhandled one.
func (r *txnRegistry) abort(ctx context.Context, key txnKey, number int64) error {
	r.rw.Lock()
	defer r.rw.Unlock()

	t := r.m[key]
	if t == nil || t.number != number {
		return errNoSuchTransaction()
	}

	if t.aborted {
		delete(r.m, key)

		return nil
	}

	r.end(ctx, key, t, "aborted by the client")

	return nil
}

// discard rolls a transaction back after one of its commands failed, and leaves a marker
// behind so the session's next command gets NoSuchTransaction rather than silence.
//
// PostgreSQL puts a connection whose statement failed into an aborted state where every
// further statement errors out until the transaction ends, so there is nothing to salvage.
// Rolling back at once also releases the locks instead of holding them until the client
// gets around to it.
func (r *txnRegistry) discard(ctx context.Context, key txnKey) {
	r.rw.Lock()
	defer r.rw.Unlock()

	t := r.m[key]
	if t == nil || t.aborted {
		return
	}

	conn := t.conn
	t.conn = nil
	t.aborted = true

	if err := r.p.Rollback(ctx, conn); err != nil {
		r.l.WarnContext(ctx, "Rollback failed", slog.Int64("txnNumber", t.number), slog.Any("error", err))

		return
	}

	r.l.DebugContext(
		ctx, "Transaction rolled back",
		slog.Int64("txnNumber", t.number), slog.String("reason", "a command inside it failed"),
	)
}

// end rolls a transaction back and forgets it. The caller must hold the lock.
func (r *txnRegistry) end(ctx context.Context, key txnKey, t *txn, reason string) {
	delete(r.m, key)

	if t.conn == nil {
		return
	}

	conn := t.conn
	t.conn = nil

	if err := r.p.Rollback(ctx, conn); err != nil {
		r.l.WarnContext(
			ctx, "Rollback failed",
			slog.Int64("txnNumber", t.number), slog.String("reason", reason), slog.Any("error", err),
		)

		return
	}

	r.l.DebugContext(
		ctx, "Transaction rolled back",
		slog.Int64("txnNumber", t.number), slog.String("reason", reason),
	)
}

// deleteExpired rolls back transactions that outlived the lifetime limit, and returns how
// many were killed. A client that vanished mid-transaction is the reason this exists.
func (r *txnRegistry) deleteExpired(ctx context.Context) int {
	r.rw.Lock()
	defer r.rw.Unlock()

	var n int

	now := time.Now()

	for key, t := range r.m {
		if !now.After(t.deadline) {
			continue
		}

		if !t.busy.TryLock() {
			// A command is running on this transaction right now, so it is alive, and its
			// connection is not ours to touch. Leave it for the next tick.
			continue
		}

		r.end(ctx, key, t, "exceeded the transaction lifetime limit")
		t.busy.Unlock()

		n++
	}

	return n
}

// stop rolls back every open transaction.
func (r *txnRegistry) stop(ctx context.Context) {
	r.rw.Lock()
	defer r.rw.Unlock()

	for key, t := range r.m {
		r.end(ctx, key, t, "the handler is shutting down")
	}
}

// txnConn returns the connection pinned to the transaction this command belongs to,
// starting the transaction if the command is the one that opens it.
//
// It returns a nil connection when the command is not transactional, in which case the
// caller takes an ordinary connection from the pool. The key comes back too, so the caller
// can discard the transaction if the command fails.
//
// A driver marks a command as transactional with `autocommit: false`; the first command of
// the transaction also carries `startTransaction: true`.
func (h *Handler) txnFor(ctx context.Context, doc *wirebson.Document) (*txn, txnKey, error) {
	var key txnKey

	autocommit, ok := doc.Get("autocommit").(bool)
	if !ok || autocommit {
		return nil, key, nil
	}

	userID, sessionID, err := h.s.CreateOrUpdateByLSID(ctx, doc)
	if err != nil {
		return nil, key, err
	}

	if sessionID == uuid.Nil {
		return nil, key, mongoerrors.New(
			mongoerrors.ErrInvalidOptions,
			"Transaction commands require a logical session (lsid)",
		)
	}

	number, ok := doc.Get("txnNumber").(int64)
	if !ok {
		return nil, key, mongoerrors.New(
			mongoerrors.ErrInvalidOptions,
			"Transaction commands require txnNumber",
		)
	}

	key = txnKey{user: userID, sessionID: sessionID}

	var t *txn

	if start, _ := doc.Get("startTransaction").(bool); start {
		t, err = h.txns.begin(ctx, key, number)
	} else {
		t, err = h.txns.get(key, number)
	}

	if err != nil {
		return nil, key, err
	}

	return t, key, nil
}

// withConn runs f on the connection pinned to this command's transaction, or on a pooled
// connection when the command is not part of one.
//
// This is the seam that makes transactions possible at all. Every command used to borrow a
// connection from the pool and hand it straight back, which is the one thing a transaction
// cannot do: its snapshot, its locks and its uncommitted rows all live inside a single
// connection, and every command of the transaction has to land on that one.
func (h *Handler) withConn(ctx context.Context, doc *wirebson.Document, f func(*pgx.Conn) error) error {
	t, key, err := h.txnFor(ctx, doc)
	if err != nil {
		return err
	}

	if t == nil {
		return h.p.WithConn(f)
	}

	t.busy.Lock()
	defer t.busy.Unlock()

	if t.conn == nil {
		// The reaper got here first, between txnFor and this lock.
		return errNoSuchTransaction()
	}

	if err = f(t.conn.Conn()); err != nil {
		err = txnError(err)

		h.txns.discard(ctx, key)

		return err
	}

	return nil
}

// txnError translates the PostgreSQL failures that end a transaction into the MongoDB
// errors a driver knows how to act on.
//
// A row locked by another transaction, a serialization failure and a deadlock are all the
// same thing to a MongoDB client: a write conflict, which is routine under concurrency and
// which the driver is expected to retry -- but only if the error is labelled transient.
func txnError(err error) error {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return err
	}

	switch pgErr.Code {
	case pgerrcode.LockNotAvailable, pgerrcode.SerializationFailure, pgerrcode.DeadlockDetected:
		e := mongoerrors.New(mongoerrors.ErrWriteConflict, "Write conflict during transaction, please retry")
		e.Labels = []string{transientTransactionError}

		return e
	default:
		return err
	}
}

// transientTransactionError tells the driver the whole transaction may be retried.
const transientTransactionError = "TransientTransactionError"

// msgCommitTransaction implements `commitTransaction` command.
func (h *Handler) msgCommitTransaction(connCtx context.Context, req *middleware.Request) (*middleware.Response, error) {
	key, number, err := h.txnTarget(connCtx, req.Document())
	if err != nil {
		return nil, err
	}

	if err = h.txns.commit(connCtx, key, number); err != nil {
		return nil, err
	}

	return middleware.ResponseDoc(req, must.NotFail(wirebson.NewDocument("ok", float64(1))))
}

// msgAbortTransaction implements `abortTransaction` command.
func (h *Handler) msgAbortTransaction(connCtx context.Context, req *middleware.Request) (*middleware.Response, error) {
	key, number, err := h.txnTarget(connCtx, req.Document())
	if err != nil {
		return nil, err
	}

	if err = h.txns.abort(connCtx, key, number); err != nil {
		return nil, err
	}

	return middleware.ResponseDoc(req, must.NotFail(wirebson.NewDocument("ok", float64(1))))
}

// txnTarget resolves the session and transaction number a commit or abort refers to.
func (h *Handler) txnTarget(ctx context.Context, doc *wirebson.Document) (txnKey, int64, error) {
	var key txnKey

	userID, sessionID, err := h.s.CreateOrUpdateByLSID(ctx, doc)
	if err != nil {
		return key, 0, err
	}

	if sessionID == uuid.Nil {
		return key, 0, mongoerrors.New(
			mongoerrors.ErrInvalidOptions,
			"Transaction commands require a logical session (lsid)",
		)
	}

	number, ok := doc.Get("txnNumber").(int64)
	if !ok {
		return key, 0, mongoerrors.New(
			mongoerrors.ErrInvalidOptions,
			"Transaction commands require txnNumber",
		)
	}

	return txnKey{user: userID, sessionID: sessionID}, number, nil
}

// errNoSuchTransaction is what a command that refers to a transaction the server does not
// have gets back. Drivers know this one: they stop retrying and surface it.
func errNoSuchTransaction() error {
	return mongoerrors.New(mongoerrors.ErrNoSuchTransaction, "Transaction is not in progress")
}

// txnAfterCommand ends the transaction a command belongs to if the command failed, and
// returns the reply the client should get -- which is not always the one the command
// produced.
//
// A failed write does not come back as a command error: it is reported inside the reply, in
// `writeErrors`, next to `ok: 1`. So it never reaches [Handler.withConn], and without this
// the transaction would stay open and usable after an operation MongoDB considers fatal to
// it -- a duplicate key, for instance.
//
// A write conflict hides in the same place, and MongoDB does not report it as a write error
// at all: it fails the whole command with WriteConflict and labels it transient, which is
// what tells the driver to retry the transaction instead of handing the failure to the
// application. So it is rewritten here.
func (h *Handler) txnAfterCommand(
	ctx context.Context,
	req *middleware.Request,
	resp *middleware.Response,
) *middleware.Response {
	if resp == nil {
		return resp
	}

	doc := req.Document()

	// commitTransaction and abortTransaction end the transaction themselves.
	switch doc.Command() {
	case "commitTransaction", "abortTransaction":
		return resp
	}

	if autocommit, ok := doc.Get("autocommit").(bool); !ok || autocommit {
		return resp
	}

	failed, conflict := inspectReply(resp.Document())
	if !failed {
		return resp
	}

	userID, sessionID, err := h.s.CreateOrUpdateByLSID(ctx, doc)
	if err != nil {
		return resp
	}

	h.txns.discard(ctx, txnKey{user: userID, sessionID: sessionID})

	if conflict {
		return middleware.ResponseErr(req, writeConflictError())
	}

	return resp
}

// writeConflictError is what a MongoDB client expects when its transaction lost a race for
// a row: a retryable failure of the whole transaction, not an error on one document.
func writeConflictError() *mongoerrors.Error {
	e := mongoerrors.New(mongoerrors.ErrWriteConflict, "Write conflict during transaction, please retry")
	e.Labels = []string{transientTransactionError}

	return e
}

// inspectReply reports whether a reply carries a failure, and whether that failure is a
// write conflict.
func inspectReply(doc *wirebson.Document) (failed, conflict bool) {
	if doc == nil {
		return false, false
	}

	if ok, _ := doc.Get("ok").(float64); ok == 0 {
		failed = true
	}

	for _, field := range []string{"writeErrors", "writeConcernError"} {
		v, ok := doc.Get(field).(wirebson.AnyArray)
		if !ok {
			continue
		}

		arr, err := v.Decode()
		if err != nil || arr.Len() == 0 {
			continue
		}

		failed = true

		for i := range arr.Len() {
			we, ok := arr.Get(i).(wirebson.AnyDocument)
			if !ok {
				continue
			}

			weDoc, err := we.Decode()
			if err != nil {
				continue
			}

			code, _ := weDoc.Get("code").(int32)
			if isConflictSQLState(code) {
				conflict = true
			}
		}
	}

	return failed, conflict
}

// isConflictSQLState reports whether a write error came from PostgreSQL refusing to wait
// for, or resolve, a lock.
//
// DocumentDB passes PostgreSQL's SQLSTATE straight through as the write error's code, and
// PostgreSQL packs a SQLSTATE into an int, six bits per character -- so the code of a write
// error is comparable against the states we care about.
func isConflictSQLState(code int32) bool {
	for _, state := range []string{
		pgerrcode.LockNotAvailable,     // the lock_timeout we set on the transaction fired
		pgerrcode.SerializationFailure, // two transactions could not be ordered
		pgerrcode.DeadlockDetected,     // they were waiting on each other
	} {
		if code == packSQLState(state) {
			return true
		}
	}

	return false
}

// packSQLState encodes a five-character SQLSTATE the way PostgreSQL's MAKE_SQLSTATE does:
// six bits per character, least significant first.
func packSQLState(state string) int32 {
	if len(state) != 5 {
		return 0
	}

	var packed int32

	for i := range 5 {
		packed |= int32((state[i]-'0')&0x3F) << (6 * i)
	}

	return packed
}
