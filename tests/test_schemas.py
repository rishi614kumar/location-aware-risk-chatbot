from adapters.schemas import GeoBundle, SourceMeta

def test_geo_bundle_fields():
    bundle = GeoBundle(bbl='123', borough='Manhattan')
    assert bundle.bbl == '123'
    assert bundle.borough == 'Manhattan'
    assert isinstance(bundle.sources, SourceMeta)

def test_source_meta_fields():
    meta = SourceMeta(geoclient='test')
    assert meta.geoclient == 'test'
