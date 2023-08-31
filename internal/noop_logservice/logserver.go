/*
 * Copyright 2021 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package noop_logservice

import (
	logV1 "github.com/nlnwa/veidemann-api/go/log/v1"
	"google.golang.org/protobuf/types/known/emptypb"
	"io"
)

type LogServer struct {
	logV1.UnimplementedLogServer
}

func New() *LogServer {
	return &LogServer{}
}

func (l *LogServer) WriteCrawlLog(stream logV1.Log_WriteCrawlLogServer) error {
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&emptypb.Empty{})
		}
		if err != nil {
			return err
		}
	}
}

func (l *LogServer) WritePageLog(stream logV1.Log_WritePageLogServer) error {
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&emptypb.Empty{})
		}
		if err != nil {
			return err
		}
	}
}

func (l *LogServer) ListPageLogs(_ *logV1.PageLogListRequest, _ logV1.Log_ListPageLogsServer) error {
	return nil
}

func (l *LogServer) ListCrawlLogs(_ *logV1.CrawlLogListRequest, _ logV1.Log_ListCrawlLogsServer) error {
	return nil
}
