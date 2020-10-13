/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package certs

import "time"

const (
	// DefaultRSAKeySize is the default key size used when created RSA keys.
	DefaultRSAKeySize = 2048

	// DefaultCertDuration is the default lifespan used when creating certificates.
	DefaultCertDuration = time.Hour * 24 * 365

	// When client certificates have less than ClientCertificateRenewalDuration
	// left before expiry, they will be regenerated.
	ClientCertificateRenewalDuration = DefaultCertDuration / 2
)
