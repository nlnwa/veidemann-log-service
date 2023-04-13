/*
 * Copyright 2023 National Library of Norway.
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
	"time"

	"github.com/nlnwa/veidemann-api/go/commons/v1"
	logV1 "github.com/nlnwa/veidemann-api/go/log/v1"
	"github.com/scylladb/gocqlx/v2"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ErrorUDT struct {
	gocqlx.UDT `json:"-"`
	Code       int32  `cql:"code" json:"code"`
	Msg        string `cql:"msg" json:"msg"`
	Detail     string `cql:"detail" json:"detail"`
}

func (e *ErrorUDT) toProto() *commons.Error {
	if e != nil {
		return &commons.Error{
			Code:   e.Code,
			Msg:    e.Msg,
			Detail: e.Detail,
		}
	}
	return nil
}

type CrawlLog struct {
	WarcId              string    `cql:"warc_id"`
	TimeStamp           time.Time `cql:"time_stamp"`
	StatusCode          int32     `cql:"status_code"`
	Size                int64     `cql:"size"`
	RequestedUri        string    `cql:"requested_uri"`
	ResponseUri         string    `cql:"response_uri"`
	DiscoveryPath       string    `cql:"discovery_path"`
	Referrer            string    `cql:"referrer"`
	ContentType         string    `cql:"content_type"`
	FetchTimeStamp      time.Time `cql:"fetch_time_stamp"`
	FetchTimeMs         int64     `cql:"fetch_time_ms"`
	BlockDigest         string    `cql:"block_digest"`
	PayloadDigest       string    `cql:"payload_digest"`
	StorageRef          string    `cql:"storage_ref"`
	RecordType          string    `cql:"record_type"`
	WarcRefersTo        string    `cql:"warc_refers_to"`
	IpAddress           string    `cql:"ip_address"`
	ExecutionId         string    `cql:"execution_id"`
	Retries             int32     `cql:"retries"`
	Error               *ErrorUDT `cql:"error"`
	JobExecutionId      string    `cql:"job_execution_id"`
	CollectionFinalName string    `cql:"collection_final_name"`
	Method              string    `cql:"method"`
}

func (c *CrawlLog) toProto() *logV1.CrawlLog {
	return &logV1.CrawlLog{
		WarcId:              c.WarcId,
		TimeStamp:           timestamppb.New(c.TimeStamp),
		StatusCode:          c.StatusCode,
		Size:                c.Size,
		RequestedUri:        c.RequestedUri,
		ResponseUri:         c.ResponseUri,
		DiscoveryPath:       c.DiscoveryPath,
		Referrer:            c.Referrer,
		ContentType:         c.ContentType,
		FetchTimeStamp:      timestamppb.New(c.FetchTimeStamp),
		FetchTimeMs:         c.FetchTimeMs,
		BlockDigest:         c.BlockDigest,
		PayloadDigest:       c.PayloadDigest,
		StorageRef:          c.StorageRef,
		RecordType:          c.RecordType,
		WarcRefersTo:        c.WarcRefersTo,
		IpAddress:           c.IpAddress,
		ExecutionId:         c.ExecutionId,
		Retries:             c.Retries,
		Error:               c.Error.toProto(),
		JobExecutionId:      c.JobExecutionId,
		CollectionFinalName: c.CollectionFinalName,
		Method:              c.Method,
	}
}

type Resource struct {
	Uri           string    `cql:"uri" json:"uri,omitempty"`
	FromCache     bool      `cql:"from_cache" json:"from_cache,omitempty"`
	Renderable    bool      `cql:"renderable" json:"renderable,omitempty"`
	ResourceType  string    `cql:"resource_type" json:"resource_type,omitempty"`
	ContentType   string    `cql:"content_type" json:"content_type,omitempty"`
	StatusCode    int32     `cql:"status_code" json:"status_code,omitempty"`
	DiscoveryPath string    `cql:"discovery_path" json:"discovery_path,omitempty"`
	WarcId        string    `cql:"warc_id" json:"warc_id,omitempty"`
	PageId        string    `cql:"page_id" json:"page_id,omitempty"`
	Referrer      string    `cql:"referrer" json:"referrer,omitempty"`
	Error         *ErrorUDT `cql:"error" json:"error,omitempty"`
	Method        string    `cql:"method" json:"method,omitempty"`
}

func (r *Resource) fromProto(proto *logV1.PageLog_Resource) *Resource {
	r.Uri = proto.Uri
	r.FromCache = proto.FromCache
	r.Renderable = proto.Renderable
	r.ResourceType = proto.ResourceType
	r.ContentType = proto.ContentType
	r.StatusCode = proto.StatusCode
	r.DiscoveryPath = proto.DiscoveryPath
	r.WarcId = proto.WarcId
	r.Referrer = proto.Referrer
	r.Method = proto.Method
	if proto.Error != nil {
		r.Error = &ErrorUDT{
			Code:   proto.Error.Code,
			Msg:    proto.Error.Msg,
			Detail: proto.Error.Detail,
		}
	}
	return r
}

func (r *Resource) toProto() *logV1.PageLog_Resource {
	return &logV1.PageLog_Resource{
		Uri:           r.Uri,
		FromCache:     r.FromCache,
		Renderable:    r.Renderable,
		ResourceType:  r.ResourceType,
		ContentType:   r.ContentType,
		StatusCode:    r.StatusCode,
		DiscoveryPath: r.DiscoveryPath,
		WarcId:        r.WarcId,
		Referrer:      r.Referrer,
		Method:        r.Method,
		Error:         r.Error.toProto(),
	}
}

type PageLog struct {
	WarcId              string   `cql:"warc_id"`
	Uri                 string   `cql:"uri"`
	ExecutionId         string   `cql:"execution_id"`
	Referrer            string   `cql:"referrer"`
	JobExecutionId      string   `cql:"job_execution_id"`
	CollectionFinalName string   `cql:"collection_final_name"`
	Method              string   `cql:"method"`
	Outlink             []string `cql:"outlink"`
}

func (p *PageLog) toProto(resources []*logV1.PageLog_Resource) *logV1.PageLog {
	return &logV1.PageLog{
		WarcId:              p.WarcId,
		Uri:                 p.Uri,
		ExecutionId:         p.ExecutionId,
		Referrer:            p.Referrer,
		JobExecutionId:      p.JobExecutionId,
		CollectionFinalName: p.CollectionFinalName,
		Method:              p.Method,
		Resource:            resources,
		Outlink:             p.Outlink,
	}
}
