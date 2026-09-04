---
name: GitHub push fallback
description: Reliable repository push path when the local shell credential helper cannot authenticate to GitHub.
---

When the local Git remote rejects credentials, an authorized GitHub connection
can publish a clean commit through the Git data API. Read the remote branch
first, require the expected parent, upload only the intended blobs, create the
tree and commit, then update the branch with `force: false`.

**Why:** The workspace may have an installed GitHub integration while the
shell's HTTPS credential helper is unavailable; forcing a new credential into
the workspace would be unsafe and could include unrelated dirty files.

**How to apply:** Preserve the local working tree, verify the remote head has
not moved, use the connection's proxy API without exposing credentials, and
verify the resulting commit's parent and file list afterward.