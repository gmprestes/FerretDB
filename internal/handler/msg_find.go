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

	"github.com/AlekSi/lazyerrors"
	"github.com/FerretDB/wire/wirebson"

	"github.com/FerretDB/FerretDB/v2/internal/handler/middleware"
)

// msgFind implements `find` command.
//
// The passed context is canceled when the client connection is closed.
func (h *Handler) msgFind(connCtx context.Context, req *middleware.Request) (*middleware.Response, error) {
	doc := req.Document()

	userID, sessionID, err := h.s.CreateOrUpdateByLSID(connCtx, doc)
	if err != nil {
		return nil, err
	}

	dbName, err := getRequiredParam[string](doc, "$db")
	if err != nil {
		return nil, err
	}

	// A read inside a transaction has to run on the transaction's connection: that is
	// where its snapshot and its own uncommitted writes are. Any cursor it opens lives
	// there too, so getMore has to come back to the same connection.
	t, key, err := h.txnFor(connCtx, doc)
	if err != nil {
		return nil, err
	}

	var page wirebson.RawDocument

	var cursorID int64

	if t != nil {
		t.busy.Lock()

		if t.conn == nil {
			t.busy.Unlock()

			return nil, errNoSuchTransaction()
		}

		page, cursorID, err = h.p.FindOnConn(connCtx, t.conn.Conn(), dbName, req.DocumentRaw())

		t.busy.Unlock()

		if err != nil {
			err = txnError(err)

			h.txns.discard(connCtx, key)

			return nil, err
		}
	} else {
		page, cursorID, err = h.p.Find(connCtx, dbName, req.DocumentRaw())
	}
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	h.s.AddCursor(connCtx, userID, sessionID, cursorID)

	return middleware.ResponseDoc(req, page)
}
