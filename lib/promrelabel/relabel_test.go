package promrelabel

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompbmarshal"
)

func TestLabelsToString(t *testing.T) {
	f := func(labels []prompbmarshal.Label, sExpected string) {
		t.Helper()
		s := labelsToString(labels)
		if s != sExpected {
			t.Fatalf("unexpected result;\ngot\n%s\nwant\n%s", s, sExpected)
		}
	}
	f(nil, "{}")
	f([]prompbmarshal.Label{
		{
			Name:  "__name__",
			Value: "foo",
		},
	}, "foo")
	f([]prompbmarshal.Label{
		{
			Name:  "foo",
			Value: "bar",
		},
	}, `{foo="bar"}`)
	f([]prompbmarshal.Label{
		{
			Name:  "foo",
			Value: "bar",
		},
		{
			Name:  "a",
			Value: "bc",
		},
	}, `{a="bc",foo="bar"}`)
	f([]prompbmarshal.Label{
		{
			Name:  "foo",
			Value: "bar",
		},
		{
			Name:  "__name__",
			Value: "xxx",
		},
		{
			Name:  "a",
			Value: "bc",
		},
	}, `xxx{a="bc",foo="bar"}`)
}

func TestApplyRelabelConfigs(t *testing.T) {
	f := func(config, metric string, isFinalize bool, resultExpected string) {
		t.Helper()
		pcs, err := ParseRelabelConfigsData([]byte(config), false)
		if err != nil {
			t.Fatalf("cannot parse %q: %s", config, err)
		}
		labels, err := parseMetricWithLabels(metric)
		if err != nil {
			t.Fatalf("cannot parse %s: %s", metric, err)
		}
		resultLabels := pcs.Apply(labels, 0, isFinalize)
		result := labelsToString(resultLabels)
		if result != resultExpected {
			t.Fatalf("unexpected result; got\n%s\nwant\n%s", result, resultExpected)
		}
	}
	t.Run("empty_relabel_configs", func(t *testing.T) {
		f("", `{}`, false, `{}`)
		f("", `{}`, true, `{}`)
		f("", `{foo="bar"}`, false, `{foo="bar"}`)
		f("", `xxx{foo="bar",__aaa="yyy"}`, false, `xxx{__aaa="yyy",foo="bar"}`)
		f("", `xxx{foo="bar",__aaa="yyy"}`, true, `xxx{foo="bar"}`)
	})
	t.Run("replace-miss", func(t *testing.T) {
		f(`
- action: replace
  target_label: bar
`, `{}`, false, `{}`)
		f(`
- action: replace
  source_labels: ["foo"]
  target_label: bar
`, `{}`, false, `{}`)
		f(`
- action: replace
  source_labels: ["foo"]
  target_label: "bar"
`, `{xxx="yyy"}`, false, `{xxx="yyy"}`)
		f(`
- action: replace
  source_labels: ["foo"]
  target_label: "bar"
  regex: ".+"
`, `{xxx="yyy"}`, false, `{xxx="yyy"}`)
	})
	t.Run("replace-if-miss", func(t *testing.T) {
		f(`
- action: replace
  if: '{foo="bar"}'
  source_labels: ["xxx", "foo"]
  target_label: "bar"
  replacement: "a-$1-b"
`, `{xxx="yyy"}`, false, `{xxx="yyy"}`)
	})
	t.Run("replace-hit", func(t *testing.T) {
		f(`
- action: replace
  source_labels: ["xxx", "foo"]
  target_label: "bar"
  replacement: "a-$1-b"
`, `{xxx="yyy"}`, false, `{bar="a-yyy;-b",xxx="yyy"}`)
	})
	t.Run("replace-if-hit", func(t *testing.T) {
		f(`
- action: replace
  if: '{xxx=~".y."}'
  source_labels: ["xxx", "foo"]
  target_label: "bar"
  replacement: "a-$1-b"
`, `{xxx="yyy"}`, false, `{bar="a-yyy;-b",xxx="yyy"}`)
	})
	t.Run("replace-remove-label-value-hit", func(t *testing.T) {
		f(`
- action: replace
  source_labels: ["foo"]
  target_label: "foo"
  regex: "xxx"
  replacement: ""
`, `{foo="xxx",bar="baz"}`, false, `{bar="baz"}`)
	})
	t.Run("replace-remove-label-value-miss", func(t *testing.T) {
		f(`
- action: replace
  source_labels: ["foo"]
  target_label: "foo"
  regex: "xxx"
  replacement: ""
`, `{foo="yyy",bar="baz"}`, false, `{bar="baz",foo="yyy"}`)
	})
	t.Run("replace-hit-remove-label", func(t *testing.T) {
		f(`
- action: replace
  source_labels: ["xxx", "foo"]
  regex: "yyy;.+"
  target_label: "foo"
  replacement: ""
`, `{xxx="yyy",foo="bar"}`, false, `{xxx="yyy"}`)
	})
	t.Run("replace-miss-remove-label", func(t *testing.T) {
		f(`
- action: replace
  source_labels: ["xxx", "foo"]
  regex: "yyy;.+"
  target_label: "foo"
  replacement: ""
`, `{xxx="yyyz",foo="bar"}`, false, `{foo="bar",xxx="yyyz"}`)
	})
	t.Run("replace-hit-target-label-with-capture-group", func(t *testing.T) {
		f(`
- action: replace
  source_labels: ["xxx", "foo"]
  target_label: "bar-$1"
  replacement: "a-$1-b"
`, `{xxx="yyy"}`, false, `{bar-yyy;="a-yyy;-b",xxx="yyy"}`)
	})
	t.Run("replace_all-miss", func(t *testing.T) {
		f(`
- action: replace_all
  source_labels: [foo]
  target_label: "bar"
`, `{}`, false, `{}`)
		f(`
- action: replace_all
  source_labels: ["foo"]
  target_label: "bar"
`, `{}`, false, `{}`)
		f(`
- action: replace_all
  source_labels: ["foo"]
  target_label: "bar"
`, `{xxx="yyy"}`, false, `{xxx="yyy"}`)
		f(`
- action: replace_all
  source_labels: ["foo"]
  target_label: "bar"
  regex: ".+"
`, `{xxx="yyy"}`, false, `{xxx="yyy"}`)
	})
	t.Run("replace_all-if-miss", func(t *testing.T) {
		f(`
- action: replace_all
  if: 'foo'
  source_labels: ["xxx"]
  target_label: "xxx"
  regex: "-"
  replacement: "."
`, `{xxx="a-b-c"}`, false, `{xxx="a-b-c"}`)
	})
	t.Run("replace_all-hit", func(t *testing.T) {
		f(`
- action: replace_all
  source_labels: ["xxx"]
  target_label: "xxx"
  regex: "-"
  replacement: "."
`, `{xxx="a-b-c"}`, false, `{xxx="a.b.c"}`)
	})
	t.Run("replace_all-if-hit", func(t *testing.T) {
		f(`
- action: replace_all
  if: '{non_existing_label=~".*"}'
  source_labels: ["xxx"]
  target_label: "xxx"
  regex: "-"
  replacement: "."
`, `{xxx="a-b-c"}`, false, `{xxx="a.b.c"}`)
	})
	t.Run("replace_all-regex-hit", func(t *testing.T) {
		f(`
- action: replace_all
  source_labels: ["xxx", "foo"]
  target_label: "xxx"
  regex: "(;)"
  replacement: "-$1-"
`, `{xxx="y;y"}`, false, `{xxx="y-;-y-;-"}`)
	})
	t.Run("replace-add-multi-labels", func(t *testing.T) {
		f(`
- action: replace
  source_labels: ["xxx"]
  target_label: "bar"
  replacement: "a-$1"
- action: replace
  source_labels: ["bar"]
  target_label: "zar"
  replacement: "b-$1"
`, `{xxx="yyy",instance="a.bc"}`, true, `{bar="a-yyy",instance="a.bc",xxx="yyy",zar="b-a-yyy"}`)
	})
	t.Run("replace-self", func(t *testing.T) {
		f(`
- action: replace
  source_labels: ["foo"]
  target_label: "foo"
  replacement: "a-$1"
`, `{foo="aaxx"}`, true, `{foo="a-aaxx"}`)
	})
	t.Run("replace-missing-source", func(t *testing.T) {
		f(`
- action: replace
  target_label: foo
  replacement: "foobar"
`, `{}`, true, `{foo="foobar"}`)
	})
	t.Run("keep_if_equal-miss", func(t *testing.T) {
		f(`
- action: keep_if_equal
  source_labels: ["foo", "bar"]
`, `{}`, true, `{}`)
		f(`
- action: keep_if_equal
  source_labels: ["xxx", "bar"]
`, `{xxx="yyy"}`, true, `{}`)
	})
	t.Run("keep_if_equal-hit", func(t *testing.T) {
		f(`
- action: keep_if_equal
  source_labels: ["xxx", "bar"]
`, `{xxx="yyy",bar="yyy"}`, true, `{bar="yyy",xxx="yyy"}`)
	})
	t.Run("drop_if_equal-miss", func(t *testing.T) {
		f(`
- action: drop_if_equal
  source_labels: ["foo", "bar"]
`, `{}`, true, `{}`)
		f(`
- action: drop_if_equal
  source_labels: ["xxx", "bar"]
`, `{xxx="yyy"}`, true, `{xxx="yyy"}`)
	})
	t.Run("drop_if_equal-hit", func(t *testing.T) {
		f(`
- action: drop_if_equal
  source_labels: [xxx, bar]
`, `{xxx="yyy",bar="yyy"}`, true, `{}`)
	})
	t.Run("keep-miss", func(t *testing.T) {
		f(`
- action: keep
  source_labels: [foo]
  regex: ".+"
`, `{}`, true, `{}`)
		f(`
- action: keep
  source_labels: [foo]
  regex: ".+"
`, `{xxx="yyy"}`, true, `{}`)
	})
	t.Run("keep-if-miss", func(t *testing.T) {
		f(`
- action: keep
  if: '{foo="bar"}'
`, `{foo="yyy"}`, false, `{}`)
	})
	t.Run("keep-if-hit", func(t *testing.T) {
		f(`
- action: keep
  if: '{foo="yyy"}'
`, `{foo="yyy"}`, false, `{foo="yyy"}`)
	})
	t.Run("keep-hit", func(t *testing.T) {
		f(`
- action: keep
  source_labels: [foo]
  regex: "yyy"
`, `{foo="yyy"}`, false, `{foo="yyy"}`)
	})
	t.Run("keep-hit-regexp", func(t *testing.T) {
		f(`
- action: keep
  source_labels: ["foo"]
  regex: ".+"
`, `{foo="yyy"}`, false, `{foo="yyy"}`)
	})
	t.Run("keep_metrics-miss", func(t *testing.T) {
		f(`
- action: keep_metrics
  regex:
  - foo
  - bar
`, `xxx`, true, `{}`)
	})
	t.Run("keep_metrics-if-miss", func(t *testing.T) {
		f(`
- action: keep_metrics
  if: 'bar'
`, `foo`, true, `{}`)
	})
	t.Run("keep_metrics-if-hit", func(t *testing.T) {
		f(`
- action: keep_metrics
  if: 'foo'
`, `foo`, true, `foo`)
	})
	t.Run("keep_metrics-hit", func(t *testing.T) {
		f(`
- action: keep_metrics
  regex:
  - foo
  - bar
`, `foo`, true, `foo`)
	})
	t.Run("drop-miss", func(t *testing.T) {
		f(`
- action: drop
  source_labels: [foo]
  regex: ".+"
`, `{}`, false, `{}`)
		f(`
- action: drop
  source_labels: [foo]
  regex: ".+"
`, `{xxx="yyy"}`, true, `{xxx="yyy"}`)
	})
	t.Run("drop-if-miss", func(t *testing.T) {
		f(`
- action: drop
  if: '{foo="bar"}'
`, `{foo="yyy"}`, true, `{foo="yyy"}`)
	})
	t.Run("drop-if-hit", func(t *testing.T) {
		f(`
- action: drop
  if: '{foo="yyy"}'
`, `{foo="yyy"}`, true, `{}`)
	})
	t.Run("drop-hit", func(t *testing.T) {
		f(`
- action: drop
  source_labels: [foo]
  regex: yyy
`, `{foo="yyy"}`, true, `{}`)
	})
	t.Run("drop-hit-regexp", func(t *testing.T) {
		f(`
- action: drop
  source_labels: [foo]
  regex: ".+"
`, `{foo="yyy"}`, true, `{}`)
	})
	t.Run("drop_metrics-miss", func(t *testing.T) {
		f(`
- action: drop_metrics
  regex:
  - foo
  - bar
`, `xxx`, true, `xxx`)
	})
	t.Run("drop_metrics-if-miss", func(t *testing.T) {
		f(`
- action: drop_metrics
  if: bar
`, `foo`, true, `foo`)
	})
	t.Run("drop_metrics-if-hit", func(t *testing.T) {
		f(`
- action: drop_metrics
  if: foo
`, `foo`, true, `{}`)
	})
	t.Run("drop_metrics-hit", func(t *testing.T) {
		f(`
- action: drop_metrics
  regex:
  - foo
  - bar
`, `foo`, true, `{}`)
	})
	t.Run("hashmod-miss", func(t *testing.T) {
		f(`
- action: hashmod
  source_labels: [foo]
  target_label: aaa
  modulus: 123
`, `{xxx="yyy"}`, false, `{aaa="81",xxx="yyy"}`)
	})
	t.Run("hashmod-if-miss", func(t *testing.T) {
		f(`
- action: hashmod
  if: '{foo="bar"}'
  source_labels: [foo]
  target_label: aaa
  modulus: 123
`, `{foo="yyy"}`, true, `{foo="yyy"}`)
	})
	t.Run("hashmod-if-hit", func(t *testing.T) {
		f(`
- action: hashmod
  if: '{foo="yyy"}'
  source_labels: [foo]
  target_label: aaa
  modulus: 123
`, `{foo="yyy"}`, true, `{aaa="73",foo="yyy"}`)
	})
	t.Run("hashmod-hit", func(t *testing.T) {
		f(`
- action: hashmod
  source_labels: [foo]
  target_label: aaa
  modulus: 123
`, `{foo="yyy"}`, true, `{aaa="73",foo="yyy"}`)
	})
	t.Run("labelmap-copy-label-if-miss", func(t *testing.T) {
		f(`
- action: labelmap
  if: '{foo="yyy",foobar="aab"}'
  regex: "foo"
  replacement: "bar"
`, `{foo="yyy",foobar="aaa"}`, true, `{foo="yyy",foobar="aaa"}`)
	})
	t.Run("labelmap-copy-label-if-hit", func(t *testing.T) {
		f(`
- action: labelmap
  if: '{foo="yyy",foobar="aaa"}'
  regex: "foo"
  replacement: "bar"
`, `{foo="yyy",foobar="aaa"}`, true, `{bar="yyy",foo="yyy",foobar="aaa"}`)
	})
	t.Run("labelmap-copy-label", func(t *testing.T) {
		f(`
- action: labelmap
  regex: "foo"
  replacement: "bar"
`, `{foo="yyy",foobar="aaa"}`, true, `{bar="yyy",foo="yyy",foobar="aaa"}`)
	})
	t.Run("labelmap-remove-prefix-dot-star", func(t *testing.T) {
		f(`
- action: labelmap
  regex: "foo(.*)"
`, `{xoo="yyy",foobar="aaa"}`, true, `{bar="aaa",foobar="aaa",xoo="yyy"}`)
	})
	t.Run("labelmap-remove-prefix-dot-plus", func(t *testing.T) {
		f(`
- action: labelmap
  regex: "foo(.+)"
`, `{foo="yyy",foobar="aaa"}`, true, `{bar="aaa",foo="yyy",foobar="aaa"}`)
	})
	t.Run("labelmap-regex", func(t *testing.T) {
		f(`
- action: labelmap
  regex: "foo(.+)"
  replacement: "$1-x"
`, `{foo="yyy",foobar="aaa"}`, true, `{bar-x="aaa",foo="yyy",foobar="aaa"}`)
	})
	t.Run("labelmap_all-if-miss", func(t *testing.T) {
		f(`
- action: labelmap_all
  if: foobar
  regex: "\\."
  replacement: "-"
`, `{foo.bar.baz="yyy",foobar="aaa"}`, true, `{foo.bar.baz="yyy",foobar="aaa"}`)
	})
	t.Run("labelmap_all-if-hit", func(t *testing.T) {
		f(`
- action: labelmap_all
  if: '{foo.bar.baz="yyy"}'
  regex: "\\."
  replacement: "-"
`, `{foo.bar.baz="yyy",foobar="aaa"}`, true, `{foo-bar-baz="yyy",foobar="aaa"}`)
	})
	t.Run("labelmap_all", func(t *testing.T) {
		f(`
- action: labelmap_all
  regex: "\\."
  replacement: "-"
`, `{foo.bar.baz="yyy",foobar="aaa"}`, true, `{foo-bar-baz="yyy",foobar="aaa"}`)
	})
	t.Run("labelmap_all-regexp", func(t *testing.T) {
		f(`
- action: labelmap_all
  regex: "ba(.)"
  replacement: "${1}ss"
`, `{foo.bar.baz="yyy",foozar="aaa"}`, true, `{foo.rss.zss="yyy",foozar="aaa"}`)
	})
	t.Run("labeldrop", func(t *testing.T) {
		f(`
- action: labeldrop
  regex: dropme
`, `{aaa="bbb"}`, true, `{aaa="bbb"}`)
		// if-miss
		f(`
- action: labeldrop
  if: foo
  regex: dropme
`, `{xxx="yyy",dropme="aaa",foo="bar"}`, false, `{dropme="aaa",foo="bar",xxx="yyy"}`)
		// if-hit
		f(`
- action: labeldrop
  if: '{xxx="yyy"}'
  regex: dropme
`, `{xxx="yyy",dropme="aaa",foo="bar"}`, false, `{foo="bar",xxx="yyy"}`)
		f(`
- action: labeldrop
  regex: dropme
`, `{xxx="yyy",dropme="aaa",foo="bar"}`, false, `{foo="bar",xxx="yyy"}`)
		// regex in single quotes
		f(`
- action: labeldrop
  regex: 'dropme'
`, `{xxx="yyy",dropme="aaa"}`, false, `{xxx="yyy"}`)
		// regex in double quotes
		f(`
- action: labeldrop
  regex: "dropme"
`, `{xxx="yyy",dropme="aaa"}`, false, `{xxx="yyy"}`)
	})
	t.Run("labeldrop-prefix", func(t *testing.T) {
		f(`
- action: labeldrop
  regex: "dropme.*"
`, `{aaa="bbb"}`, true, `{aaa="bbb"}`)
		f(`
- action: labeldrop
  regex: "dropme(.+)"
`, `{xxx="yyy",dropme-please="aaa",foo="bar"}`, false, `{foo="bar",xxx="yyy"}`)
	})
	t.Run("labeldrop-regexp", func(t *testing.T) {
		f(`
- action: labeldrop
  regex: ".*dropme.*"
`, `{aaa="bbb"}`, true, `{aaa="bbb"}`)
		f(`
- action: labeldrop
  regex: ".*dropme.*"
`, `{xxx="yyy",dropme-please="aaa",foo="bar"}`, false, `{foo="bar",xxx="yyy"}`)
	})
	t.Run("labelkeep", func(t *testing.T) {
		f(`
- action: labelkeep
  regex: "keepme"
`, `{keepme="aaa"}`, true, `{keepme="aaa"}`)
		// if-miss
		f(`
- action: labelkeep
  if: '{aaaa="awefx"}'
  regex: keepme
`, `{keepme="aaa",aaaa="awef",keepme-aaa="234"}`, false, `{aaaa="awef",keepme="aaa",keepme-aaa="234"}`)
		// if-hit
		f(`
- action: labelkeep
  if: '{aaaa="awef"}'
  regex: keepme
`, `{keepme="aaa",aaaa="awef",keepme-aaa="234"}`, false, `{keepme="aaa"}`)
		f(`
- action: labelkeep
  regex: keepme
`, `{keepme="aaa",aaaa="awef",keepme-aaa="234"}`, false, `{keepme="aaa"}`)
	})
	t.Run("labelkeep-regexp", func(t *testing.T) {
		f(`
- action: labelkeep
  regex: "keepme.*"
`, `{keepme="aaa"}`, true, `{keepme="aaa"}`)
		f(`
- action: labelkeep
  regex: "keepme.*"
`, `{keepme="aaa",aaaa="awef",keepme-aaa="234"}`, false, `{keepme="aaa",keepme-aaa="234"}`)
	})
	t.Run("upper-lower-case", func(t *testing.T) {
		f(`
- action: uppercase
  source_labels: ["foo"]
  target_label: foo
`, `{foo="bar"}`, true, `{foo="BAR"}`)
		f(`
- action: lowercase
  source_labels: ["foo", "bar"]
  target_label: baz
- action: labeldrop
  regex: foo|bar
`, `{foo="BaR",bar="fOO"}`, true, `{baz="bar;foo"}`)
		f(`
- action: lowercase
  source_labels: ["foo"]
  target_label: baz
- action: uppercase
  source_labels: ["bar"]
  target_label: baz
`, `{qux="quux"}`, true, `{qux="quux"}`)
	})
	t.Run("graphite-match", func(t *testing.T) {
		f(`
- action: graphite
  match: foo.*.baz
  labels:
    __name__: aaa
    job: ${1}-zz
`, `foo.bar.baz`, true, `aaa{job="bar-zz"}`)
	})
	t.Run("graphite-mismatch", func(t *testing.T) {
		f(`
- action: graphite
  match: foo.*.baz
  labels:
    __name__: aaa
    job: ${1}-zz
`, `foo.bar.bazz`, true, `foo.bar.bazz`)
	})
	t.Run("replacement-with-label-refs", func(t *testing.T) {
		// no regex
		f(`
- target_label: abc
  replacement: "{{__name__}}.{{foo}}"
`, `qwe{foo="bar",baz="aaa"}`, true, `qwe{abc="qwe.bar",baz="aaa",foo="bar"}`)
		// with regex
		f(`
- target_label: abc
  replacement: "{{__name__}}.{{foo}}.$1"
  source_labels: [baz]
  regex: "a(.+)"
`, `qwe{foo="bar",baz="aaa"}`, true, `qwe{abc="qwe.bar.aa",baz="aaa",foo="bar"}`)
	})
}

func TestFinalizeLabels(t *testing.T) {
	f := func(metric, resultExpected string) {
		t.Helper()
		labels, err := parseMetricWithLabels(metric)
		if err != nil {
			t.Fatalf("cannot parse %s: %s", metric, err)
		}
		resultLabels := FinalizeLabels(nil, labels)
		result := labelsToString(resultLabels)
		if result != resultExpected {
			t.Fatalf("unexpected result; got\n%s\nwant\n%s", result, resultExpected)
		}
	}
	f(`{}`, `{}`)
	f(`{foo="bar",__aaa="ass",instance="foo.com"}`, `{foo="bar",instance="foo.com"}`)
	f(`{foo="bar",instance="ass",__address__="foo.com"}`, `{foo="bar",instance="ass"}`)
	f(`{foo="bar",abc="def",__address__="foo.com"}`, `{abc="def",foo="bar"}`)
}

func TestRemoveMetaLabels(t *testing.T) {
	f := func(metric, resultExpected string) {
		t.Helper()
		labels, err := parseMetricWithLabels(metric)
		if err != nil {
			t.Fatalf("cannot parse %s: %s", metric, err)
		}
		resultLabels := RemoveMetaLabels(nil, labels)
		result := labelsToString(resultLabels)
		if result != resultExpected {
			t.Fatalf("unexpected result of RemoveMetaLabels;\ngot\n%s\nwant\n%s", result, resultExpected)
		}
	}
	f(`{}`, `{}`)
	f(`{foo="bar"}`, `{foo="bar"}`)
	f(`{__meta_foo="bar"}`, `{}`)
	f(`{__meta_foo="bdffr",foo="bar",__meta_xxx="basd"}`, `{foo="bar"}`)
}

func TestFillLabelReferences(t *testing.T) {
	f := func(replacement, metric, resultExpected string) {
		t.Helper()
		labels, err := parseMetricWithLabels(metric)
		if err != nil {
			t.Fatalf("cannot parse %s: %s", metric, err)
		}
		result := fillLabelReferences(nil, replacement, labels)
		if string(result) != resultExpected {
			t.Fatalf("unexpected result; got\n%q\nwant\n%q", result, resultExpected)
		}
	}
	f(``, `foo{bar="baz"}`, ``)
	f(`abc`, `foo{bar="baz"}`, `abc`)
	f(`foo{{bar`, `foo{bar="baz"}`, `foo{{bar`)
	f(`foo-$1`, `foo{bar="baz"}`, `foo-$1`)
	f(`foo{{bar}}`, `foo{bar="baz"}`, `foobaz`)
	f(`{{bar}}`, `foo{bar="baz"}`, `baz`)
	f(`{{bar}}-aa`, `foo{bar="baz"}`, `baz-aa`)
	f(`{{bar}}-aa{{__name__}}.{{bar}}{{non-existing-label}}`, `foo{bar="baz"}`, `baz-aafoo.baz`)
}

func TestRegexpMatchStringSuccess(t *testing.T) {
	f := func(pattern, s string) {
		t.Helper()
		rc := &RelabelConfig{
			Action: "labeldrop",
			Regex: &MultiLineRegex{
				S: pattern,
			},
		}
		prc, err := parseRelabelConfig(rc)
		if err != nil {
			t.Fatalf("unexpected error in parseRelabelConfig: %s", err)
		}
		if !prc.matchString(s) {
			t.Fatalf("unexpected matchString(%q) result; got false; want true", s)
		}
	}
	f("", "")
	f("foo", "foo")
	f(".*", "")
	f(".*", "foo")
	f("foo.*", "foobar")
	f("foo.+", "foobar")
	f("f.+o", "foo")
	f("foo|bar", "bar")
	f("^(foo|bar)$", "foo")
	f("foo.+", "foobar")
	f("^foo$", "foo")
}

func TestRegexpMatchStringFailure(t *testing.T) {
	f := func(pattern, s string) {
		t.Helper()
		rc := &RelabelConfig{
			Action: "labeldrop",
			Regex: &MultiLineRegex{
				S: pattern,
			},
		}
		prc, err := parseRelabelConfig(rc)
		if err != nil {
			t.Fatalf("unexpected error in parseRelabelConfig: %s", err)
		}
		if prc.matchString(s) {
			t.Fatalf("unexpected matchString(%q) result; got true; want false", s)
		}
	}
	f("", "foo")
	f("foo", "")
	f("foo.*", "foa")
	f("foo.+", "foo")
	f("f.+o", "foor")
	f("foo|bar", "barz")
	f("^(foo|bar)$", "xfoo")
	f("foo.+", "foo")
	f("^foo$", "foobar")
}
