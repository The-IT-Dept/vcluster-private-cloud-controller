// Package hostname implements the hostname authority: the check that decides
// whether a guest cluster is entitled to publish a given hostname on the host.
//
// This is the multi-tenant boundary. Every tenant has cluster-admin on their
// own cluster and can write any Ingress, Gateway listener or HTTPRoute they
// like; without this check, tenant A publishes an Ingress for tenant B's
// hostname and the host serves it.
package hostname

import (
	"fmt"
	"strings"
)

// Allowed reports whether hostname is authorized by at least one pattern.
//
// Matching rules, chosen to be exactly what DNS and TLS certificates do so
// that nobody has to learn a third wildcard dialect:
//
//   - A plain pattern matches only itself (case-insensitive, ignoring a
//     trailing dot).
//   - A "*." prefix matches exactly ONE label: "*.a.example.com" matches
//     "x.a.example.com" but NOT "x.y.a.example.com" and NOT "a.example.com".
//     This is the restrictive reading. DNS zone wildcards (RFC 4592) match
//     multiple labels, but granting that here would let one pattern authorize
//     an unbounded namespace of names the operator never reviewed.
//   - A hostname that is itself a wildcard (a guest Ingress may legitimately
//     carry host: "*.a.example.com") is authorized only by an IDENTICAL
//     pattern. It must never match a broader wildcard: "*.example.com" must
//     not authorize publishing "*.other.example.com" — or the guest would gain
//     every name under it in one move.
//   - The empty hostname matches nothing. An Ingress rule with no host matches
//     every hostname the controller serves, which is precisely the thing this
//     package exists to forbid.
func Allowed(hostname string, patterns []string) bool {
	h := canonical(hostname)
	if h == "" {
		return false
	}
	hostIsWildcard := strings.Contains(h, "*")
	for _, pat := range patterns {
		p := canonical(pat)
		if p == "" {
			continue
		}
		if h == p {
			return true
		}
		if hostIsWildcard {
			// Only the identical-pattern grant above can authorize a wildcard
			// hostname; falling through would let the one-label match treat "*"
			// as an ordinary label.
			continue
		}
		if rest, ok := strings.CutPrefix(p, "*."); ok {
			dot := strings.IndexByte(h, '.')
			if dot > 0 && h[dot+1:] == rest {
				return true
			}
		}
	}
	return false
}

// Refusal builds the message a tenant sees when a hostname is rejected. It
// names the hostname AND the allowed domains, because "refused" alone leaves a
// customer diffing strings against documentation to find out why.
func Refusal(hostname string, patterns []string) string {
	if len(patterns) == 0 {
		return fmt.Sprintf("hostname %q refused: this cluster has no allowed domains configured", hostname)
	}
	return fmt.Sprintf("hostname %q refused: not covered by this cluster's allowed domains [%s] (wildcards match one label)",
		hostname, strings.Join(patterns, ", "))
}

// canonical lowercases and strips the trailing root dot, the two equivalences
// DNS itself has. Nothing else: an invalid name simply won't match anything.
func canonical(s string) string {
	return strings.ToLower(strings.TrimSuffix(strings.TrimSpace(s), "."))
}
