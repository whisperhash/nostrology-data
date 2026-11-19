// Mint-URL canonicalization - the draft-cashu-charge profile, adopted verbatim:
// lowercase scheme and host, drop the default port
// (443/80), strip trailing slashes, then exact string match. This MUST be
// specified or the allowlist is bypassable by casing: NUT-00's only mandated
// normalization is trailing-slash stripping, and its own example carries a
// non-default port, so "obvious" normalizations differ between implementations.
//
// Deliberately NOT cashu-ts's normalizeMintUrl: our allowlist comparison is a
// security boundary and must not drift with a library bump. The rule is 12
// lines; owning it is cheaper than pinning someone else's.

export function canonicalMintUrl(raw: string): string {
  const url = new URL(raw);
  const scheme = url.protocol.toLowerCase(); // includes the trailing ':'
  if (scheme !== 'https:' && scheme !== 'http:') {
    throw new Error(`mint URL scheme must be http(s), got ${scheme}`);
  }
  const host = url.hostname.toLowerCase();
  const isDefaultPort =
    url.port === '' || (scheme === 'https:' && url.port === '443') || (scheme === 'http:' && url.port === '80');
  const port = isDefaultPort ? '' : `:${url.port}`;
  // Path case is preserved: Minibits' path is '/Bitcoin' and paths are
  // case-sensitive per RFC 3986 - only scheme/host are case-insensitive.
  const path = url.pathname.replace(/\/+$/, '');
  return `${scheme}//${host}${port}${path}`;
}

export function isAllowlistedMint(raw: string, allowlist: readonly string[]): boolean {
  let canonical: string;
  try {
    canonical = canonicalMintUrl(raw);
  } catch {
    return false;
  }
  return allowlist.includes(canonical);
}
