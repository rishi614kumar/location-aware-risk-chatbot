from adapters.schemas import GeoBundle
import scripts.GeoScope as GeoScope


class DummyDataset:
    def __init__(self, name: str) -> None:
        self.name = name


class DummyHandler:
    def __init__(self, dataset_name: str) -> None:
        self._datasets = [DummyDataset(dataset_name)]

    def __iter__(self):
        return iter(self._datasets)


def test_get_dataset_filters_returns_bundles(monkeypatch):
    dataset_name = "Test Dataset"
    addresses = [
        {
            "house_number": "123",
            "street_name": "Main St",
            "borough": "Queens",
        }
    ]

    monkeypatch.setitem(
        GeoScope.DATASET_CONFIG,
        dataset_name,
        {"geo_unit": "PRECINCT", "surrounding": False},
    )

    monkeypatch.setattr(
        GeoScope,
        "get_bbl_from_address",
        lambda address, borough: "1000000001",
    )

    monkeypatch.setattr(
        GeoScope,
        "geo_from_bbl",
        lambda bbl: GeoBundle(bbl=bbl, precinct="99"),
    )

    handler = DummyHandler(dataset_name)

    filters, bundles = GeoScope.get_dataset_filters(addresses, handler, surrounding=False)

    assert dataset_name in filters
    assert filters[dataset_name]["where"] == "Precinct IN ('99')"
    assert bundles and isinstance(bundles[0], GeoBundle)
