.PHONY: ztf-hats gaia-hats gaia-ztf-xmatched

ztf-hats:
	uv run dg launch --assets ztf_hats --config config.yaml

gaia-hats:
	uv run dg launch --assets gaia_hats --config config.yaml

gaia-ztf-xmatched:
	uv run dg launch --assets gaia_ztf_xmatched --config config.yaml