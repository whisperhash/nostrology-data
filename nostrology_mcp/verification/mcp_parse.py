"""Parse an MCP tool response into its envelope, column contract and CSV table."""
import csv
import io
import json
import re


class Resp:
    """One tool response: `.env`, `.rows`/`.hdr`, `.error`, `.truncated`."""

    def __init__(self, call, body):
        self.call = call
        self.body = body
        self.error = body.startswith("TOOLERROR:") or body.startswith("ERROR:")
        self.rows, self.hdr, self.env = [], [], {}
        self.methodology = ""
        self.truncated = []
        if self.error:
            return
        lines = body.splitlines()
        mi = next((n for n, l in enumerate(lines) if l.startswith("methodology")), None)
        for line in lines[:mi]:
            if ":" in line:
                k, v = line.split(":", 1)
                self.env[k.strip()] = v.strip()
        if mi is not None:
            self.methodology = lines[mi]
            table = lines[mi + 2:]
        else:
            table = []
        self.truncated = [l for l in lines if l.startswith("truncated:")]
        table = [l for l in table if not l.startswith("truncated:")]
        text = "\n".join(table)
        if text.strip():
            parsed = list(csv.reader(io.StringIO(text)))
            self.hdr, self.rows = parsed[0], [r for r in parsed[1:] if r]

    # -- accessors ---------------------------------------------------------
    def col(self, name):
        i = self.hdr.index(name)
        return [r[i] for r in self.rows]

    def dicts(self):
        return [dict(zip(self.hdr, r)) for r in self.rows]

    def declared_rows(self):
        m = re.search(r"rows:\s*(\d+)", self.body)
        return int(m.group(1)) if m else None

    def as_of_day(self):
        line = next((l for l in self.body.splitlines() if l.startswith("as_of")), "")
        m = re.search(r"(\d{4}-\d\d-\d\d)T", line)
        return m.group(1) if m else None

    def series(self, valcol=None):
        """utc_day -> int, for a daily metric."""
        vc = valcol or self.hdr[1]
        return {d: int(v) for d, v in zip(self.col("utc_day"), self.col(vc))}

    def to_json(self):
        return {"call": self.call, "body": self.body}


def load(path):
    raw = open(path).read()
    head, body = raw.split("\n", 1)
    return Resp(json.loads(head[len("### CALL "):]), body)


def save(resp, path):
    with open(path, "w") as fh:
        fh.write("### CALL " + json.dumps(resp.call) + "\n" + resp.body)
