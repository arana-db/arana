Contributing to Arana

## 1. Branch

  >- The name of branches `SHOULD` be in the format of `feature/xxx`.
  >- You `SHOULD` checkout a new branch after a feature branch already being merged into upstream, `DO NOT` commit in the old branch.

## 2. Pull Request

### 2.1. Title Format

The pr head format is `<head> <subject>`. The title should be simpler to show your intent.

The title format of the pull request `MUST` follow the following rules:

  >- Start with `Doc:` for adding/formatting/improving docs.
  >- Start with `Mod:` for formatting codes or adding comment.
  >- Start with `Fix:` for fixing bug, and its ending should be ` #issue-id` if being relevant to some issue.
  >- Start with `Imp:` for improving performance.
  >- Start with `Ftr:` for adding a new feature.
  >- Start with `Add:` for adding struct function/member.
  >- Start with `Rft:` for refactoring codes.
  >- Start with `Tst:` for adding tests.
  >- Start with `Dep:` for adding depending libs.
  >- Start with `Rem:` for removing feature/struct/function/member/files.

## 3. Code Style

Please add a blank line at the end of every file.

Please add a blank line at the sub-title and its content.

Please add s space between the Engilsh word/digit number and the Chinese character.


### import

We import blocks should be splited into 3 blocks.

```Go
// block 1: the go internal package
import (
	"fmt"
)

// block 2: the third package
import (
	"github.com/pkg/errors"

	"gopkg.in/yaml.v3"
)

// block 3: the arana package
import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/proto/rule"
)
```

## 4. Tools

There are some aided development tools.

### golangci-lint

```shell
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
```

### license-eye

```shell
go install github.com/apache/skywalking-eyes/cmd/license-eye@latest
```

### imports-formatter

```shell
go install github.com/dubbogo/tools/cmd/imports-formatter@latest
```

### pre-commit
A framework for managing and maintaining multi-language pre-commit hooks. [Pre-commit](https://pre-commit.com/index.html) is highly recommended.

Using pip:
```shell
pip install pre-commit
```
Using homebrew:

```shell
brew install pre-commit
```
