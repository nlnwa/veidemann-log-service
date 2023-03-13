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

package logservice

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nlnwa/veidemann-api/go/commons/v1"
	logV1 "github.com/nlnwa/veidemann-api/go/log/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func BenchmarkCollectCrawlLog(b *testing.B) {
	for n := 0; n < b.N; n++ {
		CollectCrawlLog(crawlLog1)
	}
}

func BenchmarkCollectPageLog(b *testing.B) {
	for n := 0; n < b.N; n++ {
		CollectPageLog(pageLog1)
	}
}

func timestamp() *timestamppb.Timestamp {
	return timestamppb.New(time.Now().UTC().Truncate(time.Millisecond))
}

var (
	crawlLog1 = &logV1.CrawlLog{
		ExecutionId:         uuid.NewString(),
		JobExecutionId:      uuid.NewString(),
		WarcId:              uuid.NewString(),
		FetchTimeStamp:      timestamp(),
		BlockDigest:         "sha1:f054ed8f9fd5893d6b70dc336a68e8092782723c",
		CollectionFinalName: "Collection_2021",
		ContentType:         "text/dns",
		DiscoveryPath:       "P",
		FetchTimeMs:         46,
		IpAddress:           "8.8.8.8:53",
		RecordType:          "response",
		RequestedUri:        "dns:www.example.com",
		Size:                50,
		Error: &commons.Error{
			Code:   -1,
			Msg:    "Error",
			Detail: "Everything went wrong",
		},
		StatusCode: 1,
		StorageRef: "warcfile:Collection_2021-20210415110455-veidemann_contentwriter_775ffd88bc_5ljbb-00000.warc.gz:667",
	}
	crawlLog2 = &logV1.CrawlLog{
		ExecutionId:         crawlLog1.ExecutionId,
		JobExecutionId:      crawlLog1.JobExecutionId,
		WarcId:              uuid.NewString(),
		FetchTimeStamp:      timestamp(),
		BlockDigest:         "sha1:f054ed8f9fd5893d6b70dc336a68e8092782723c",
		CollectionFinalName: "Collection_2021",
		ContentType:         "text/dns",
		DiscoveryPath:       "P",
		FetchTimeMs:         46,
		IpAddress:           "8.8.8.8:53",
		RecordType:          "response",
		RequestedUri:        "dns:www.example.com",
		Size:                50,
		StatusCode:          200,
		StorageRef:          "warcfile:Collection_2021-20210415110455-veidemann_contentwriter_775ffd88bc_5ljbb-00000.warc.gz:667",
	}
	pageLog1 = &logV1.PageLog{
		ExecutionId:         uuid.NewString(),
		WarcId:              uuid.NewString(),
		JobExecutionId:      uuid.NewString(),
		Uri:                 "https://www.nb.no/samlinger",
		Referrer:            "https://www.nb.no/",
		CollectionFinalName: "Veidemann_2021",
		Method:              "GET",
		Resource: []*logV1.PageLog_Resource{
			{
				Uri:           "https://www.nb.no/samlinger",
				FromCache:     false,
				Renderable:    false,
				ResourceType:  "t",
				ContentType:   "text/html",
				StatusCode:    200,
				DiscoveryPath: "L",
				WarcId:        uuid.NewString(),
				Referrer:      "https://www.nb.no/",
				Error:         nil,
				Method:        "GET",
			},
		},
		Outlink: []string{
			"https://www.nb.no/whatever",
		},
	}
	pageLog2 = &logV1.PageLog{
		WarcId:              uuid.NewString(),
		ExecutionId:         pageLog1.ExecutionId,
		JobExecutionId:      pageLog1.JobExecutionId,
		Uri:                 "https://www.nb.no/presse",
		Referrer:            "https://www.nb.no/",
		CollectionFinalName: "Veidemann_2021",
		Method:              "GET",
		Resource: []*logV1.PageLog_Resource{
			{
				Uri:           "https://www.nb.no/presse",
				FromCache:     false,
				Renderable:    false,
				ResourceType:  "t",
				ContentType:   "text/html",
				StatusCode:    200,
				DiscoveryPath: "L",
				WarcId:        uuid.NewString(),
				Referrer:      "https://www.nb.no/",
				Error:         nil,
				Method:        "GET",
			},
		},
		Outlink: []string{
			"https://www.nb.no/foobar",
		},
	}
)
