import asyncio
from asyncio.events import get_event_loop
from time import time

# import numpy as np
import requests
from aiohttp.client import ClientSession
from loguru import logger


class YandexResponse:
    def __init__(self, response: dict):
        self.response = response

    def _feature_member(self):
        if "response" not in self.response:
            return None
        return self.response["response"]["GeoObjectCollection"]["featureMember"]

    def _geo_object(self):
        return feature_member[0]["GeoObject"] if (feature_member := self._feature_member()) else None

    @staticmethod
    def _get_point(location: str):
        loc = location.split(" ")
        return {"lat": float(loc[1]), "lon": float(loc[0])}

    def bounded_by(self):
        if not (geo_object := self._geo_object()):
            return None
        if not (envelope := geo_object.get("boundedBy", {}).get("Envelope", {})):
            return None
        return {"lower": self._get_point(envelope["lowerCorner"]), "upper": self._get_point(envelope["upperCorner"])}

    def point(self):
        if not (geo_object := self._geo_object()):
            return None
        return self._get_point(geo_object["Point"]["pos"])

    def config(self):
        if not (geo_object := self._geo_object()):
            return None
        if not (meta_data_property := geo_object.get("metaDataProperty", {})):
            return None
        address = meta_data_property.get("GeocoderMetaData", {}).get("Address", {})
        return {
            "name": geo_object.get("name"),
            "formatted": address.get("formatted"),
            "postal_code": address.get("postal_code"),
            "bounded_by": self.bounded_by(),
            "config_raw": {item["kind"]: item["name"] for item in address.get("Components", [])},
        }


class YandexGeocoder:
    __geocoder = "https://geocode-maps.yandex.ru/1.x/"

    def __init__(self, api_key, api_key_reserve):
        self.__api_key = api_key
        self.__api_key_reserve = api_key_reserve

    def from_address(self, address: str) -> dict:
        params = {"format": "json", "apikey": self.__api_key, "geocode": address}
        r = requests.get(self.__geocoder, params=params)
        if r.status_code == 403:  # если получили ошибку 403 - ключ сдох, используем второй
            params = {"format": "json", "apikey": self.__api_key_reserve, "geocode": address}
            r = requests.get(self.__geocoder, params=params)
        logger.info(f"Yandex API, get from address, request: {r.status_code}, {r.reason}")
        return r.json()

    def from_point(self, lat: float, lon: float) -> dict:
        params = {"format": "json", "apikey": self.__api_key, "geocode": f"{lat},{lon}", "sco": "latlong", "results": 1}
        r = requests.get(self.__geocoder, params=params)
        if r.status_code == 403:  # если получили ошибку 403 - ключ сдох, используем второй
            params = {
                "format": "json",
                "apikey": self.__api_key,
                "geocode": f"{lat},{lon}",
                "sco": "latlong",
                "results": 1,
            }
            r = requests.get(self.__geocoder, params=params)
        logger.info(f"Yandex API, get from point, request: {r.status_code}, {r.reason}")
        return r.json()

    def get_point_response(self, address: str) -> YandexResponse:
        return YandexResponse(self.from_address(address))

    @staticmethod
    def get_point(result: dict):
        if "response" not in result or not result["response"]["GeoObjectCollection"]["featureMember"]:
            return None
        tmp = result["response"]["GeoObjectCollection"]
        loc = tmp["featureMember"][0]["GeoObject"]["Point"]["pos"].split(" ")
        return float(loc[1]), float(loc[0])

    @staticmethod
    def get_location(result: dict):
        if "response" not in result or not result["response"]["GeoObjectCollection"]["featureMember"]:
            return None
        tmp = result["response"]["GeoObjectCollection"]
        loc = tmp["featureMember"][0]["GeoObject"]["Point"]["pos"].split(" ")
        return {"lat": float(loc[1]), "lon": float(loc[0])}

    @staticmethod
    def get_address(result: dict):
        if "response" not in result or not result["response"]["GeoObjectCollection"]["featureMember"]:
            return None
        tmp = result["response"]["GeoObjectCollection"]
        return tmp["featureMember"][0]["GeoObject"]["metaDataProperty"]["GeocoderMetaData"]["text"]

    @staticmethod
    def get_city(result: dict):
        if "response" not in result or not result["response"]["GeoObjectCollection"]["featureMember"]:
            return None
        tmp = result["response"]["GeoObjectCollection"]
        address = tmp["featureMember"][0]["GeoObject"]["metaDataProperty"]["GeocoderMetaData"]["Address"]
        return next(x["name"] for x in address["Components"] if x["kind"] == "locality")

    @staticmethod
    def get_state(result: dict):
        if "response" not in result or not result["response"]["GeoObjectCollection"]["featureMember"]:
            return None
        tmp = result["response"]["GeoObjectCollection"]
        address = tmp["featureMember"][0]["GeoObject"]["metaDataProperty"]["GeocoderMetaData"]["Address"]
        return next(x["name"] for x in address["Components"] if x["kind"] == "province")

    @staticmethod
    def get_postal_code(result: dict):
        if "response" not in result or not result["response"]["GeoObjectCollection"]["featureMember"]:
            return None
        tmp = result["response"]["GeoObjectCollection"]
        address = tmp["featureMember"][0]["GeoObject"]["metaDataProperty"]["GeocoderMetaData"]["Address"]
        return address["postal_code"]

    def batch_geocode(self, addresses):
        logger.info(f"Started batch geocoder; addresses: {len(addresses)}")
        start_t = time()

        async def runner():
            async with ClientSession() as session:
                return await self.download(session, addresses)

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = get_event_loop().run_until_complete(runner())
        logger.info(f"Geocoder geocoded all, elapsed:{time() - start_t:0.2f} sec")
        return result

    async def download(self, session, addresses, batch=300):
        address_set = list(set(addresses))
        size = len(address_set)
        result = {}

        for start in range(0, size, batch):
            addr_slice = address_set[start : start + batch]  # noqa
            result |= {address: await self.__geocode(session, address) for address in addr_slice}

        return result

    async def __geocode(self, session, address: str):
        params = {"format": "json", "apikey": self.__api_key, "geocode": address}
        try:
            result = await self.__get(self.__geocoder, session, params, self.__api_key_reserve)
        except Exception as exc:
            logger.warning(exc)
            return None
        return self.get_location(result)

    @staticmethod
    async def __get(url: str, session: ClientSession, params: dict, api_key_reserve: str = "") -> dict:
        async with session.get(url, params=params) as resp:
            # если получили ошибку 403 и есть другой ключ - пробуем с ним
            if resp.status == 403 and api_key_reserve != "":
                params["apikey"] = api_key_reserve
                return await YandexGeocoder.__get(url, session, params)
            elif resp.status != 200:
                raise Exception(f"Response error: {resp.status} - {resp.reason}")
            return await resp.json()
