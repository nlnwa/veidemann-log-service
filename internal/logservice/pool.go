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

import "github.com/scylladb/gocqlx/v2"

// Pool holds instances of *gocqlx.Queryx.
type Pool struct {
	pool chan *gocqlx.Queryx
	new  func() *gocqlx.Queryx
}

// NewPool creates a new pool of *gocqlx.Queryx.
func NewPool(size int, new func() *gocqlx.Queryx) *Pool {
	return &Pool{
		new:  new,
		pool: make(chan *gocqlx.Queryx, size),
	}
}

// Borrow a *gocqlx.Queryx from the pool.
func (p *Pool) Borrow() *gocqlx.Queryx {
	select {
	case c := <-p.pool:
		return c
	default:
		return p.new()
	}
}

// Return returns a *gocqlx.Queryx to the pool.
func (p *Pool) Return(c *gocqlx.Queryx) {
	select {
	case p.pool <- c:
	default:
		c.Release()
	}
}

func (p *Pool) Drain() {
	// must be sure no one is returning while draining
	close(p.pool)
	for c := range p.pool {
		c.Release()
	}
}
