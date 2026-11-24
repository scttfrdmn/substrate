# A `List<String>` and [a link](https://x.invalid) — done

Each heading below is a rule pageAnchors has to reproduce. `make docs-anchor-fixtures`
renders this page with VitePress and writes the ids it produced to headings.golden.json.

## The decoder's own error text

## 400 is `ValidationError`

## Cost

## Cost

## Cost

## Foo &amp; bar\_baz <span>tag</span> &copy;

## `aws:` and `ec2:Subnet`

## Café naïve

```
## Not a heading
```

~~~~md
```
## Still not a heading
~~~~

## Closing hashes ##

## The **Arn**s list

## Intra*word*emphasis and snake_case_name

## __Bold__ and _em_ and a_b_c and _not closed

## ***Triple*** and **unclosed and *a **b** c*

## Rule *of **three***

## ~~Struck~~ and ~single~ and ~~~odd~~~

## Custom {#custom-id}

## With class {.big}

## Both {.big #two-attrs}

## Not attrs {}

## Key value {data-x=y}

## Later {#later}

## Later

## Image ![alt text](img.png) here

## ![only an image](img.png)

## Emoji :smile: and :not_an_emoji: and :+1:

## Shortcut :) and a:)b and <3 and :D!

## [Ref text][ref] and [shortcut] and [collapsed][] and [Case Folded][REF]

## [Undefined][nope] and [also undefined]

## Inline [link](https://x.invalid "title") and [empty]() done

## Link with *emph* [a *b* c](u) end

## [*open](u) close*

## Escaped \*star\* and \_under\_ and \:smile\:

## `code *not emph*` and *em `code` em*

## Autolink <https://x.invalid/a> and <b>tag</b> and x < y

## **:smile:** inside strong

## Nested [outer [inner] text](u)

A paragraph with `<a id="in-code">` in a code span, which is not an anchor.

<a id="custom-anchor"></a> <span id='single-quoted'>x</span> <a name="named-anchor"></a>
<div class="x" id="later-attr"></div>

[ref]: https://x.invalid/ref
[shortcut]: https://x.invalid/s
[collapsed]: https://x.invalid/c
