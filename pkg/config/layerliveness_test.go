// Copyright 2026 TeamSquad
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

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLayerLivenessConfig(t *testing.T) {
	t.Run("off by default", func(t *testing.T) {
		conf, err := NewConfig("", true, nil, nil)
		require.NoError(t, err)
		require.False(t, conf.LayerLiveness.Metrics)
		require.False(t, conf.LayerLiveness.APIFilter)
	})

	t.Run("the metric switch stands alone", func(t *testing.T) {
		conf, err := NewConfig("layer_liveness:\n  metrics: true\n", true, nil, nil)
		require.NoError(t, err)
		require.True(t, conf.LayerLiveness.Metrics)
		require.False(t, conf.LayerLiveness.APIFilter)
	})

	// AST-449. The filter is gone, but the field is not: config parsing is STRICT by default
	// (cmd/server: strictMode = !--disable-strict-config), so a node whose livekit.yaml still
	// says api_filter would refuse to boot if the field were deleted. Every origin of the va
	// fleet carries it today, so this is the test that keeps the next restart from being an
	// outage. NewLivekitServer warns when it is set; here we only pin that it parses.
	t.Run("the retired API filter still parses under strict config", func(t *testing.T) {
		conf, err := NewConfig("layer_liveness:\n  api_filter: true\n", true, nil, nil)
		require.NoError(t, err)
		require.True(t, conf.LayerLiveness.APIFilter)
	})

	t.Run("and it no longer drags a prerequisite behind it", func(t *testing.T) {
		conf, err := NewConfig(
			"api:\n  enable_psrpc_for_get_list_participants: true\nlayer_liveness:\n  api_filter: true\n",
			true, nil, nil,
		)
		require.NoError(t, err)
		require.True(t, conf.LayerLiveness.APIFilter)
		require.True(t, conf.API.EnablePsrpcForGetListParticpants)
	})

	// CONTROL NEGATIVO del control anterior: an unknown key under the same block DOES fail, so
	// the test above is proving that api_filter is accepted, not that strict mode is off.
	t.Run("strict mode is really on", func(t *testing.T) {
		_, err := NewConfig("layer_liveness:\n  api_filtr: true\n", true, nil, nil)
		require.Error(t, err)
	})
}
