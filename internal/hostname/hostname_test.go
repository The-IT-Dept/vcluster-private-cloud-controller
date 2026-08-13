package hostname

import (
	"strings"
	"testing"
)

// TestAllowed is the exhaustive table for the hostname authority. Every row
// states why it must hold, because each wrong answer here is a cross-tenant
// publish (false positive) or a broken customer (false negative).
func TestAllowed(t *testing.T) {
	cases := []struct {
		name     string
		hostname string
		patterns []string
		want     bool
		why      string
	}{
		// --- exact matches ---
		{"exact match", "app.example.com", []string{"app.example.com"}, true,
			"a literal grant must work"},
		{"exact mismatch", "other.example.com", []string{"app.example.com"}, false,
			"a literal grant covers one name only"},
		{"apex granted", "example.com", []string{"example.com"}, true,
			"an apex can be granted literally"},
		{"case-insensitive host", "APP.Example.COM", []string{"app.example.com"}, true,
			"DNS names are case-insensitive; refusing on case would break real clients"},
		{"case-insensitive pattern", "app.example.com", []string{"APP.EXAMPLE.COM"}, true,
			"the operator's casing must not matter either"},
		{"trailing dot on host", "app.example.com.", []string{"app.example.com"}, true,
			"the root dot is the same DNS name"},
		{"trailing dot on pattern", "app.example.com", []string{"app.example.com."}, true,
			"same equivalence, other side"},

		// --- wildcard depth: matches ONE label, as DNS certificates do ---
		{"wildcard one label", "x.a.example.com", []string{"*.a.example.com"}, true,
			"the documented behaviour: one label under the suffix"},
		{"wildcard two labels", "x.y.a.example.com", []string{"*.a.example.com"}, false,
			"a wildcard must NOT match deeper names — that would grant an unbounded namespace"},
		{"wildcard zero labels", "a.example.com", []string{"*.a.example.com"}, false,
			"the wildcard's own suffix is not covered by the wildcard"},
		{"wildcard wrong suffix", "x.b.example.com", []string{"*.a.example.com"}, false,
			"a sibling domain is another tenant's namespace"},
		{"wildcard suffix must align on label boundary", "x.aa.example.com", []string{"*.a.example.com"}, false,
			"'aa.example.com' merely ends with 'a.example.com' as a string; matching it would leak into a different domain"},
		{"wildcard case-insensitive", "X.A.Example.Com", []string{"*.a.example.com"}, true,
			"case-insensitivity applies to wildcard matches too"},
		{"top-level wildcard", "app.example.com", []string{"*.example.com"}, true,
			"one label under the apex"},
		{"top-level wildcard depth", "app.sub.example.com", []string{"*.example.com"}, false,
			"still one label only, wherever the wildcard sits"},

		// --- wildcard hostnames (the guest publishes host: "*.something") ---
		{"identical wildcard grant", "*.a.example.com", []string{"*.a.example.com"}, true,
			"granting the wildcard itself is a legitimate, explicit operator decision"},
		{"wildcard host under broader wildcard", "*.a.example.com", []string{"*.example.com"}, false,
			"a broader wildcard must not authorize publishing a narrower one: that hands over every name under it"},
		{"wildcard host under exact grant", "*.example.com", []string{"example.com"}, false,
			"an apex grant is one name, not its subtree"},
		{"deeper wildcard host", "*.y.a.example.com", []string{"*.a.example.com"}, false,
			"same one-label rule, expressed as a hostname"},

		// --- degenerate inputs, all of which must refuse ---
		{"empty hostname", "", []string{"*.example.com"}, false,
			"an Ingress rule with no host matches every hostname — the exact thing to forbid"},
		{"whitespace hostname", "   ", []string{"*.example.com"}, false,
			"same as empty after canonicalisation"},
		{"no patterns", "app.example.com", nil, false,
			"no allowed domains means nothing is allowed; default-open would be the vulnerability"},
		{"empty pattern entry", "app.example.com", []string{""}, false,
			"an empty pattern must not become a match-everything grant"},
		{"bare star pattern ignored", "app.example.com", []string{"*."}, false,
			"'*.' has an empty suffix; treating it as match-all would defeat the authority"},
		{"bare star hostname", "*", []string{"*.example.com"}, false,
			"a lone star is not a name"},
		{"pattern with only whitespace", "app.example.com", []string{"  "}, false,
			"whitespace is not a grant"},

		// --- multiple patterns: any one grant suffices ---
		{"second pattern matches", "x.a.example.com", []string{"b.example.com", "*.a.example.com"}, true,
			"the list is a union of grants"},
		{"none of several", "x.example.net", []string{"a.example.com", "*.example.com"}, false,
			"a different registrable domain matches nothing here"},

		// --- shapes that look close but are different names ---
		{"superstring host", "app.example.com.attacker.example", []string{"app.example.com"}, false,
			"a name that merely CONTAINS the grant is a different name — the classic phishing shape"},
		{"substring host", "example.com", []string{"app.example.com"}, false,
			"the parent of a granted name is not granted"},
		{"port is not part of a hostname", "app.example.com:443", []string{"app.example.com"}, false,
			"Ingress hosts never carry ports; anything that does is malformed and must not match"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Allowed(tc.hostname, tc.patterns); got != tc.want {
				t.Errorf("Allowed(%q, %v) = %v, want %v — %s", tc.hostname, tc.patterns, got, tc.want, tc.why)
			}
		})
	}
}

func TestRefusalNamesTheEvidence(t *testing.T) {
	// The refusal message is a customer-facing surface: it must name BOTH the
	// hostname and the allowed domains, or the customer is left guessing which
	// side to fix.
	msg := Refusal("evil.example.net", []string{"*.a.example.com", "a.example.com"})
	for _, want := range []string{"evil.example.net", "*.a.example.com", "a.example.com"} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal %q does not name %q", msg, want)
		}
	}
}
