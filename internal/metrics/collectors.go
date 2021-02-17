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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/version"
)

const namespace = "veidemann"

var collectors = map[string]prometheus.Collector{
	"uri.requests": prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "uri",
		Name:      "requests_total",
		Help:      "The total number of uris requested"}),
	"uri.requests.failed": prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "uri",
		Name:      "requests_failed_total",
		Help:      "The total number of failed uri requests"}),
	"uri.statuscode": prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "uri",
			Name:      "statuscode_total",
			Help:      "The total number of responses for each status code",
		},
		[]string{"code"}),
	"uri.mime": prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "uri",
			Name:      "mime_type_total",
			Help:      "The total number of responses for each mime type",
		},
		[]string{"mime"}),
	"uri.recordtype": prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "uri",
			Name:      "record_type_total",
			Help:      "The total number of responses for each record type",
		},
		[]string{"type"}),
	"uri.fetchtime": prometheus.NewSummary(
		prometheus.SummaryOpts{
			Namespace: namespace,
			Subsystem: "uri",
			Name:      "fetch_time_seconds",
			Help:      "The time used for fetching the uri in seconds",
		}),
	"uri.size": prometheus.NewSummary(
		prometheus.SummaryOpts{
			Namespace: namespace,
			Subsystem: "uri",
			Name:      "size_bytes",
			Help:      "Fetched content size in bytes",
		}),
	"page.requests": prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "page",
		Name:      "requests_total",
		Help:      "The total number of pages requested"}),
	"page.outlinks": prometheus.NewSummary(
		prometheus.SummaryOpts{
			Namespace: namespace,
			Subsystem: "page",
			Name:      "outlinks_total",
			Help:      "Outlinks per page",
		}),
	"page.resources": prometheus.NewSummary(
		prometheus.SummaryOpts{
			Namespace: namespace,
			Subsystem: "page",
			Name:      "resources_total",
			Help:      "Resources loaded per page",
		}),
	"page.resources.cache.hit": prometheus.NewSummary(
		prometheus.SummaryOpts{
			Namespace: namespace,
			Subsystem: "page",
			Name:      "resources_cache_hit_total",
			Help:      "Resources loaded from cache per page",
		}),
	"page.resources.cache.miss": prometheus.NewSummary(
		prometheus.SummaryOpts{
			Namespace: namespace,
			Subsystem: "page",
			Name:      "resources_cache_miss_total",
			Help:      "Resources loaded from origin server per page",
		}),
	"page.links": prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "page",
			Name:      "links_total",
			Help:      "Total number of outlinks and resources",
		},
		[]string{"type"}),
}

func registerCollectors() {
	prometheus.MustRegister(version.NewCollector("veidemann-log-service"))

	for i := range collectors {
		collector := collectors[i]
		prometheus.MustRegister(collector)
	}
}
