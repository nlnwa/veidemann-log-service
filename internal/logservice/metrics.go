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
	"fmt"
	logV1 "github.com/nlnwa/veidemann-api/go/log/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/version"
	"strings"
)

const Namespace = "veidemann"

var (
	UriRequests = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: Namespace,
		Subsystem: "uri",
		Name:      "requests_total",
		Help:      "The total number of uris requested",
	})

	UriRequestsFailed = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: Namespace,
		Subsystem: "uri",
		Name:      "requests_failed_total",
		Help:      "The total number of failed uri requests",
	})

	UriStatusCode = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace,
		Subsystem: "uri",
		Name:      "statuscode_total",
		Help:      "The total number of responses for each status code",
	}, []string{"code"})

	UriRecordType = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace,
		Subsystem: "uri",
		Name:      "record_type_total",
		Help:      "The total number of responses for each record type",
	}, []string{"type"})

	UriMimeType = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace,
		Subsystem: "uri",
		Name:      "mime_type_total",
		Help:      "The total number of responses for each mime type",
	}, []string{"mime"})

	UriFetchTimeSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: Namespace,
		Subsystem: "uri",
		Name:      "fetch_time_seconds",
		Help:      "The time used for fetching the uri in seconds",
	})

	UriSizeBytes = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: Namespace,
		Subsystem: "uri",
		Name:      "size_bytes",
		Help:      "Fetched content size in bytes",
	})

	PageRequests = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: Namespace,
		Subsystem: "page",
		Name:      "requests_total",
		Help:      "The total number of pages requested"})

	PageOutlinks = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: Namespace,
		Subsystem: "page",
		Name:      "outlinks_total",
		Help:      "Outlinks per page",
	})

	PageResources = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: Namespace,
		Subsystem: "page",
		Name:      "resources_total",
		Help:      "Resources loaded per page",
	})

	PageResourcesCacheHit = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: Namespace,
		Subsystem: "page",
		Name:      "resources_cache_hit_total",
		Help:      "Resources loaded from cache per page",
	})

	PageResourcesCacheMiss = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: Namespace,
		Subsystem: "page",
		Name:      "resources_cache_miss_total",
		Help:      "Resources loaded from origin server per page",
	})

	PageLinks = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace,
		Subsystem: "page",
		Name:      "links_total",
		Help:      "Total number of outlinks and resources",
	}, []string{"type"})
)

func init() {
	prometheus.MustRegister(version.NewCollector("veidemann_log_service"))
}

func CollectCrawlLog(crawlLog *logV1.CrawlLog) {
	UriRequests.Inc()
	if crawlLog.GetError() != nil {
		UriRequestsFailed.Inc()
	}
	UriStatusCode.WithLabelValues(fmt.Sprint(crawlLog.GetStatusCode())).Inc()
	UriRecordType.WithLabelValues(fmt.Sprint(crawlLog.GetRecordType())).Inc()
	if mime := getNormalizedMimeType(crawlLog.GetContentType()); len(mime) > 0 {
		UriMimeType.WithLabelValues(mime).Inc()
	}
	fetchTimeMs := crawlLog.GetFetchTimeMs()
	if fetchTimeMs > 0 {
		UriFetchTimeSeconds.Observe(float64(fetchTimeMs) / 1000)
	}
	size := crawlLog.GetSize()
	if size > 0 {
		UriSizeBytes.Observe(float64(size))
	}
}

func CollectPageLog(pageLog *logV1.PageLog) {
	PageRequests.Inc()

	outlinks := pageLog.GetOutlink()
	if len(outlinks) > 0 {
		PageOutlinks.Observe(float64(len(outlinks)))
		PageLinks.WithLabelValues("outlinks").Add(float64(len(outlinks)))
	}
	resources := pageLog.GetResource()
	if len(resources) > 0 {
		var cached float64
		var notCached float64
		for _, resource := range resources {
			if resource.GetFromCache() == true {
				cached++
			} else {
				notCached++
			}
		}
		PageResources.Observe(cached + notCached)
		PageResourcesCacheHit.Observe(cached)
		PageResourcesCacheMiss.Observe(notCached)
		PageLinks.WithLabelValues("resources_notcached").Add(notCached)
		PageLinks.WithLabelValues("resources_cached").Add(cached)
	}
}

func getNormalizedMimeType(contentType string) string {
	if len(contentType) > 0 {
		i := strings.Index(contentType, ";")
		if i > 0 {
			return contentType[:i]
		}
	}
	return contentType
}
