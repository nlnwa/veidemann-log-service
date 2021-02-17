/*
 * Copyright 2018 National Library of Norway.
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

package metrics

import (
	"fmt"
	logV1 "github.com/nlnwa/veidemann-api/go/log/v1"
	"github.com/prometheus/client_golang/prometheus"
	"strings"
)

func init() {
	registerCollectors()
}

func CollectCrawlLog(crawlLog *logV1.CrawlLog) {
	collectors["uri.requests"].(prometheus.Counter).Inc()
	if crawlLog.GetError() != nil {
		collectors["uri.requests.failed"].(prometheus.Counter).Inc()
	}
	collectors["uri.statuscode"].(*prometheus.CounterVec).WithLabelValues(fmt.Sprint(crawlLog.GetStatusCode())).Inc()
	collectors["uri.recordtype"].(*prometheus.CounterVec).WithLabelValues(fmt.Sprint(crawlLog.GetRecordType())).Inc()
	if mime := getNormalizedMimeType(crawlLog.GetContentType()); len(mime) > 0 {
		collectors["uri.mime"].(*prometheus.CounterVec).WithLabelValues(mime).Inc()
	}
	fetchTimeMs := crawlLog.GetFetchTimeMs()
	if fetchTimeMs > 0 {
		collectors["uri.fetchtime"].(prometheus.Summary).Observe(float64(fetchTimeMs) / 1000)
	}
	size := crawlLog.GetSize()
	if size > 0 {
		collectors["uri.size"].(prometheus.Summary).Observe(float64(size))
	}
}

func CollectPageLog(pageLog *logV1.PageLog) {
	collectors["page.requests"].(prometheus.Counter).Inc()

	outlinks := pageLog.GetOutlink()
	if len(outlinks) > 0 {
		collectors["page.outlinks"].(prometheus.Summary).Observe(float64(len(outlinks)))
		collectors["page.links"].(*prometheus.CounterVec).WithLabelValues("outlinks").Add(float64(len(outlinks)))
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
		collectors["page.resources"].(prometheus.Summary).Observe(cached + notCached)
		collectors["page.resources.cache.hit"].(prometheus.Summary).Observe(cached)
		collectors["page.resources.cache.miss"].(prometheus.Summary).Observe(notCached)
		collectors["page.links"].(*prometheus.CounterVec).WithLabelValues("resources_notcached").Add(notCached)
		collectors["page.links"].(*prometheus.CounterVec).WithLabelValues("resources_cached").Add(cached)
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
