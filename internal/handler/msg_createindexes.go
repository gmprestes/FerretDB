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
	"time"

	"github.com/AlekSi/lazyerrors"
	"github.com/FerretDB/wire/wirebson"
	"github.com/jackc/pgx/v5"

	"github.com/FerretDB/FerretDB/v2/internal/documentdb/documentdb_api"
	"github.com/FerretDB/FerretDB/v2/internal/documentdb/documentdb_api_internal"
	"github.com/FerretDB/FerretDB/v2/internal/handler/middleware"
	"github.com/FerretDB/FerretDB/v2/internal/mongoerrors"
	"github.com/FerretDB/FerretDB/v2/internal/util/logging"
	"github.com/FerretDB/FerretDB/v2/internal/util/must"
)

// indexBuildPollInterval is how often the queued index build is checked for
// completion while the createIndexes command waits for it.
const indexBuildPollInterval = 500 * time.Millisecond

// msgCreateIndexes implements `createIndexes` command.
//
// The passed context is canceled when the client connection is closed.
func (h *Handler) msgCreateIndexes(connCtx context.Context, req *middleware.Request) (*middleware.Response, error) {
	doc := req.Document()

	if _, _, err := h.s.CreateOrUpdateByLSID(connCtx, doc); err != nil {
		return nil, err
	}

	dbName, err := getRequiredParam[string](doc, "$db")
	if err != nil {
		return nil, err
	}

	v := doc.Get("indexes")
	if v == nil {
		return nil, mongoerrors.NewWithArgument(
			mongoerrors.ErrLocation40414,
			"BSON field 'createIndexes.indexes' is missing but a required field",
			"indexes",
		)
	}

	var res wirebson.AnyDocument

	err = h.p.WithConn(func(conn *pgx.Conn) error {
		res, err = h.createIndexes(connCtx, conn, doc.Command(), dbName, req.DocumentRaw())
		return err
	})
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	return middleware.ResponseDoc(req, res)
}

// createIndexes creates indexes via DocumentDB's background build queue and
// waits for the build to complete, so the command keeps MongoDB semantics
// (returns when indexes are ready) without holding a server-side transaction
// for the whole build. The build itself survives a client disconnect: it is
// queued in a durable catalog table and executed by the extension's background
// worker (documentdb.indexBuildsScheduledOnBgWorker must be on).
//
// This replaces the blocking create_indexes_non_concurrently path, which held
// the client connection silent for the entire build — long builds (large
// collections during restores) were killed by connection timeouts and left
// collections without their indexes.
func (h *Handler) createIndexes(connCtx context.Context, conn *pgx.Conn, command, dbName string, spec wirebson.RawDocument) (wirebson.AnyDocument, error) { //nolint:lll // for readability
	resRaw, ok, requests, err := documentdb_api.CreateIndexesBackground(connCtx, conn, h.L, dbName, spec)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	if ok {
		if failRaw, err := h.waitForIndexBuild(connCtx, conn, command, requests); err != nil {
			return nil, lazyerrors.Error(err)
		} else if failRaw != nil {
			resRaw = failRaw
		}
	}

	return h.decodeCreateIndexesResponse(connCtx, command, resRaw)
}

// waitForIndexBuild polls check_build_index_status until the queued build
// completes. It returns a non-nil raw response when the build failed (to be
// decoded and mapped like any createIndexes response), following the polling
// protocol of DocumentDB's own create_indexes_background test helper.
func (h *Handler) waitForIndexBuild(connCtx context.Context, conn *pgx.Conn, command string, requests wirebson.RawDocument) (wirebson.RawDocument, error) { //nolint:lll // for readability
	reqDoc, err := requests.Decode()
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	// No queued request: the indexes already existed, nothing to wait for.
	if reqDoc.Get("indexRequest") == nil && reqDoc.Get("indexRequests") == nil {
		return nil, nil
	}

	start := time.Now()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	for {
		statusRaw, ok, complete, err := documentdb_api_internal.CheckBuildIndexStatus(connCtx, conn, h.L, requests)
		if err != nil {
			return nil, lazyerrors.Error(err)
		}

		if !ok {
			h.L.WarnContext(connCtx, "CreateIndexes background build failed",
				slog.String("command", command), slog.Duration("elapsed", time.Since(start)))
			return statusRaw, nil
		}

		if complete {
			h.L.InfoContext(connCtx, "CreateIndexes background build complete",
				slog.String("command", command), slog.Duration("elapsed", time.Since(start)))
			return nil, nil
		}

		select {
		case <-connCtx.Done():
			// The client went away; the queued build keeps running server-side
			// and the index appears when it finishes.
			h.L.InfoContext(connCtx, "CreateIndexes client disconnected, background build continues",
				slog.String("command", command), slog.Duration("elapsed", time.Since(start)))
			return nil, connCtx.Err()
		case <-logEvery.C:
			h.L.InfoContext(connCtx, "CreateIndexes background build in progress",
				slog.String("command", command), slog.Duration("elapsed", time.Since(start)))
		case <-time.After(indexBuildPollInterval):
		}
	}
}

// decodeCreateIndexesResponse decodes a DocumentDB createIndexes response and
// maps an embedded error to a command error if any.
func (h *Handler) decodeCreateIndexesResponse(connCtx context.Context, command string, resRaw wirebson.RawDocument) (wirebson.AnyDocument, error) { //nolint:lll // for readability
	// TODO https://github.com/FerretDB/FerretDB-DocumentDB/issues/292

	res, err := resRaw.DecodeDeep()
	if err != nil {
		h.L.WarnContext(connCtx, "CreateIndexes failed to decode response", logging.Error(err), slog.String("command", command))
		return resRaw, nil
	}

	lazyRes := slog.Any("res", logging.LazyString(res.LogMessage))

	h.L.DebugContext(connCtx, "CreateIndexes raw response", lazyRes, slog.String("command", command))

	raw, _ := res.Get("raw").(*wirebson.Document)
	if raw == nil {
		h.L.WarnContext(connCtx, "CreateIndexes: unexpected response", lazyRes, slog.String("command", command))
		return res, nil
	}

	defaultShard, _ := raw.Get("defaultShard").(*wirebson.Document)
	if defaultShard == nil {
		h.L.WarnContext(connCtx, "CreateIndexes: unexpected response", lazyRes, slog.String("command", command))
		return res, nil
	}

	c, _ := defaultShard.Get("code").(int32)
	code := mongoerrors.MapWrappedCode(c)

	if code != 0 {
		errMsg, _ := defaultShard.Get("errmsg").(string)
		return nil, mongoerrors.New(code, errMsg)
	}

	resOk := defaultShard.Get("ok").(int32)
	must.NoError(defaultShard.Replace("ok", float64(resOk)))

	return defaultShard, nil
}
