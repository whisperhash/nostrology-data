// Structured JSON log lines on stdout -> docker -> the log shipper -> Loki (the same
// zero-config path every container on the box gets). The iron rule, as in
// CLAUDE.md (logs are shipped to a store that others read): NO
// SECRETS. Nothing here may ever receive a token string, a proof secret, a
// blinding factor, or a constructed proof - callers pass the token HASH and
// counts. The `fields` type is deliberately primitive-only to make "just log
// the proofs object" a type error rather than a review catch.

type LogFields = Record<string, string | number | boolean | null>;

function emit(level: 'info' | 'warn' | 'error', msg: string, fields: LogFields): void {
  console.log(
    JSON.stringify({ ts: new Date().toISOString(), level, service: 'cashu-redeemer', msg, ...fields }),
  );
}

export const log = {
  info: (msg: string, fields: LogFields = {}) => emit('info', msg, fields),
  warn: (msg: string, fields: LogFields = {}) => emit('warn', msg, fields),
  error: (msg: string, fields: LogFields = {}) => emit('error', msg, fields),
};
