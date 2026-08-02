package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/common/expfmt"
)

func TestManifestFamilyExported(t *testing.T) {
	r, err := New("t", 1, "", 0)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	r.ManifestDivergence("shard_bits", "peer-disagree")
	r.ManifestAdoption("source_mode", "bootstrap")
	r.ManifestReshardEmitDuplicate()

	mfs, err := r.promReg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var sb strings.Builder
	enc := expfmt.NewEncoder(&sb, expfmt.NewFormat(expfmt.TypeTextPlain))
	for _, mf := range mfs {
		_ = enc.Encode(mf)
	}
	out := sb.String()
	for _, want := range []string{
		"multicast_manifest_divergence_total",
		"multicast_manifest_adoption_total",
		"multicast_manifest_last_divergence_epoch",
		"multicast_manifest_pilots_known",
		"multicast_manifest_resharding_emit_duplicates_total",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %s from /metrics", want)
		}
	}
	if !strings.Contains(out, `field="shard_bits"`) {
		t.Errorf("last_divergence_epoch missing field label; got:\n%s", out)
	}
}
