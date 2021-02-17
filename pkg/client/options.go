/*
 * Copyright 2020 National Library of Norway.
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

package client

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"strconv"
	"time"
)

// options configure a connection. options are set by the Option
// values passed to NewConnectionOptions.
type options struct {
	name           string
	host           string
	port           int
	connectTimeout time.Duration
	dialOptions    []grpc.DialOption
}

// Option configures how to dial to a service.
type Option interface {
	apply(*options)
}

// EmptyOption does not alter the configuration. It can be embedded in
// another structure to build custom connection options.
type EmptyOption struct{}

func (EmptyOption) apply(*options) {}

// funcOption wraps a function that modifies options into an
// implementation of the Option interface.
type funcOption struct {
	f func(*options)
}

func (fco *funcOption) apply(po *options) {
	fco.f(po)
}

func newFuncOption(f func(*options)) *funcOption {
	return &funcOption{
		f: f,
	}
}

func defaultOptions(serviceName string) options {
	return options{
		name:           serviceName,
		connectTimeout: 10 * time.Second,
	}
}

func (opts *options) addr() string {
	return opts.host + ":" + strconv.Itoa(opts.port)
}

func (opts *options) dial() (*grpc.ClientConn, error) {
	dialOpts := append(opts.dialOptions,
		grpc.WithInsecure(),
		grpc.WithBlock(),
	)

	dialCtx, dialCancel := context.WithTimeout(context.Background(), opts.connectTimeout)
	defer dialCancel()
	clientConn, err := grpc.DialContext(dialCtx, opts.addr(), dialOpts...)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("failed to dial %s at %s within %s: %s", opts.name, opts.addr(),
				opts.connectTimeout, err)
		}
		return nil, err
	}
	return clientConn, nil
}

func WithHost(host string) Option {
	return newFuncOption(func(c *options) {
		c.host = host
	})
}

func WithPort(port int) Option {
	return newFuncOption(func(c *options) {
		c.port = port
	})
}

func WithDialOptions(dialOption ...grpc.DialOption) Option {
	return newFuncOption(func(c *options) {
		c.dialOptions = dialOption
	})
}

func WithConnectTimeout(connectTimeout time.Duration) Option {
	return newFuncOption(func(c *options) {
		c.connectTimeout = connectTimeout
	})
}
