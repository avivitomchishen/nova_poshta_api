from __future__ import annotations

import base64
import math
import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

import requests
from http import HTTPStatus as Status


class PersonProperty(str, Enum):
    SENDER = "Sender"
    RECIPIENT = "Recipient"


class PersonType(str, Enum):
    PRIVATE = "PrivatePerson"
    COMPANY = "Organization"


class CargoType(str, Enum):
    CARGO = "Cargo"
    PARCEL = "Parcel"
    MONEY = "Money"


class ServiceType(str, Enum):
    WAREHOUSE_WAREHOUSE = "WarehouseWarehouse"


class PaymentMethod(str, Enum):
    CASH = "Cash"
    NON_CASH = "NonCash"


class WarehouseType(str, Enum):
    POST_MACHINE = "Postomat"


@dataclass
class NovaPoshtaSender:
    city_identifier: str
    agent_identifier: str
    address_identifier: str
    agent_contact_identifier: str
    phone: str
    address: str
    department_number: int
    max_weight_allowed: float


@dataclass
class DeliveryReceiver:
    phone: str


@dataclass
class NovaPoshtaReceiver:
    city_identifier: str
    agent_identifier: str
    address_identifier: str
    agent_contact_identifier: str
    address: str
    department_number: int
    max_weight_allowed: float
    delivery_receiver: DeliveryReceiver


@dataclass
class DeliveryInfo:
    ttn: str
    estimate_order_price: float
    payer: str  # PersonProperty.SENDER.value / RECIPIENT.value
    comment: str = ""

    def get_payment_method_display(self) -> str:
        return PaymentMethod.NON_CASH.value


@dataclass
class NovaPoshta:
    sender: NovaPoshtaSender
    receiver: NovaPoshtaReceiver
    specified_weight: float
    with_backward_delivery: bool
    backward_amount: float
    postpaid_amount: float
    delivery_identifier: str
    delivery_date: datetime
    delivery: DeliveryInfo


class NovaPoshtaAPI:
    BASE_URL = "https://api.novaposhta.ua/v2.0/json/"
    MY_NP_URL = "https://my.novaposhta.ua"

    def __init__(self, api_key: str, timeout: int = 20):
        self.api_key = api_key
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json;charset=utf-8"})

    def _post(self, model: str, method: str, props: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        payload = {"apiKey": self.api_key, "modelName": model, "calledMethod": method, "methodProperties": props or {}}
        try:
            r = self.session.post(self.BASE_URL, json=payload, timeout=self.timeout)
            r.raise_for_status()
            body = r.json()
        except requests.HTTPError as e:
            return {"status": False, "status_code": getattr(e.response, "status_code", 400), "error": str(e)}
        except requests.RequestException as e:
            return {"status": False, "status_code": 400, "error": str(e)}

        if not body.get("success", False):
            msg = "; ".join(body.get("errors") or []) or "; ".join(body.get("messageCodes") or []) or "API error"

            code = 401 if "API key" in msg and "invalid" in msg.lower() else 400
            return {"status": False, "status_code": code, "error": msg, "raw": body}

        return {"status": True, "status_code": Status.OK, **body}

    @staticmethod
    def _list_ok(resp: Dict[str, Any], not_found_msg: str) -> Dict[str, Any]:
        if not resp.get("status"):
            return resp
        data = resp.get("data") or []
        return (
            {"status": False, "status_code": Status.NOT_FOUND, "error": not_found_msg}
            if not data
            else {"status": True, "data": data, "info": resp.get("info", {})}
        )

    @staticmethod
    def _map_city(c: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "city_ref": c.get("Ref"),
            "city": c.get("Description"),
            "area": c.get("AreaDescription"),
            "settlement_type": c.get("SettlementTypeDescription"),
        }

    @staticmethod
    def _map_wh(w: Dict[str, Any]) -> Dict[str, Any]:
        place = int(w.get("PlaceMaxWeightAllowed") or 0)
        total = int(w.get("TotalMaxWeightAllowed") or 0)
        return {
            "number": int(w.get("Number") or 0),
            "address_ref": w.get("Ref"),
            "address": w.get("Description"),
            "index": w.get("WarehouseIndex"),
            "city_ref": w.get("CityRef"),
            "city": w.get("CityDescription"),
            "max_weight_allowed": total or place,
            "max_weight_allowed_place": place,
            "max_weight_allowed_total": total,
        }

    @staticmethod
    def _ceil(total: int, size: int) -> int:
        size = max(1, int(size or 1))
        return math.ceil(total / size)

    @staticmethod
    def _fname(cd: str, fallback: str) -> str:
        m = re.search(r'filename="?([^"]+)"?', cd or "")
        return m.group(1) if m else fallback

    def find_city_by_name(self, city: str, limit: int = 50, page: int = 1) -> Dict[str, Any]:
        res = self._post("Address", "getCities", {"FindByString": city, "Limit": limit, "Page": page})
        chk = self._list_ok(res, f'City named "{city}" is not found')
        if not chk.get("status"):
            return chk

        data: List[Dict[str, Any]] = chk["data"]
        exact = next((x for x in data if (x.get("Description") or "").lower() == city.lower()), None)
        cities = [self._map_city(exact)] if exact else [self._map_city(x) for x in data]
        total = 1 if exact else (chk.get("info", {}).get("totalCount") or len(cities))
        return {
            "status": True,
            "status_code": Status.OK,
            "cities": cities,
            "total": total,
            "total_pages": self._ceil(total, limit),
            "total_in_page": len(cities),
            "current_page": page,
        }

    def find_warehouse_in_city(
            self,
            city_ref: Optional[str] | None,
            warehouse_number: Optional[int] | None,
            warehouse_string: Optional[str] = None,
            city: str = "",
            limit: int = 50,
            page: int = 1,
    ) -> Dict[str, Any]:
        props: Dict[str, Any] = {"Limit": limit, "Page": page}
        if city_ref:
            props["CityRef"] = city_ref
        if warehouse_number is not None:
            props["WarehouseId"] = warehouse_number
        if warehouse_string:
            props["FindByString"] = warehouse_string

        res = self._post("AddressGeneral", "getWarehouses", props)
        label_city = f' in "{city}"' if city else ""
        label_wh = f"#{warehouse_number}" if warehouse_number is not None else ""
        chk = self._list_ok(res, f"Warehouse {label_wh} is not found{label_city}")
        if not chk.get("status"):
            return chk

        data = chk["data"]
        total = chk.get("info", {}).get("totalCount", len(data))
        if total <= 1 and data:
            wh = self._map_wh(data[0])
            return {
                "status": True,
                "status_code": Status.OK,
                "warehouses": wh,
                "total": total,
                "total_pages": 1,
                "total_in_page": 1,
                "current_page": 1,
            }
        warehouses = [self._map_wh(x) for x in data]
        return {
            "status": True,
            "status_code": Status.OK,
            "warehouses": warehouses,
            "total": total,
            "total_pages": self._ceil(total, limit),
            "total_in_page": len(warehouses),
            "current_page": page,
        }

    def find_agents_by_property(self, property: PersonProperty = PersonProperty.RECIPIENT, page: int = 1):
        res = self._post("Counterparty", "getCounterparties", {"CounterpartyProperty": property.value, "Page": page})
        chk = self._list_ok(res, f'Agents by "{property}" property is not found')
        if not chk.get("status"):
            return chk
        return {
            "status": True,
            "status_code": Status.OK,
            "agents": chk["data"],
            "total": chk.get("info", {}).get("totalCount", len(chk["data"])),
        }

    def create_contact(
            self,
            first_name: str,
            last_name: str,
            phone: str,
            city_ref: str = "",
            middle_name: str = "",
            email: str = "",
            edrpou: str = "",
            counterparty_type: PersonType = PersonType.PRIVATE,
            counterparty_property: PersonProperty = PersonProperty.RECIPIENT,
    ) -> Dict[str, Any]:
        return self._post(
            "Counterparty",
            "save",
            {
                "FirstName": first_name,
                "MiddleName": middle_name,
                "LastName": last_name,
                "Phone": phone,
                "Email": email,
                "CounterpartyType": counterparty_type.value,
                "CounterpartyProperty": counterparty_property.value,
                "CityRef": city_ref,
                "EDRPOU": edrpou,
            },
        )

    def update_contact(
            self,
            agent_ref: str,
            contact_ref: str,
            first_name: str,
            last_name: str,
            middle_name: str,
            phone: str,
            email: str = "",
    ) -> Dict[str, Any]:
        return self._post(
            "ContactPerson",
            "update",
            {
                "Ref": contact_ref,
                "CounterpartyRef": agent_ref,
                "FirstName": first_name,
                "MiddleName": middle_name,
                "LastName": last_name,
                "Phone": phone,
                "Email": email,
            },
        )

    def delete_contact(self, contact_ref: str) -> Dict[str, Any]:
        return self._post("ContactPerson", "delete", {"Ref": contact_ref})

    def update_agent(
            self,
            agent_ref: str,
            city_ref: str,
            first_name: str,
            last_name: str,
            middle_name: str,
            phone: str,
            email: str = "",
            counterparty_type: PersonType = PersonType.PRIVATE,
            counterparty_property: PersonProperty = PersonProperty.RECIPIENT,
    ) -> Dict[str, Any]:
        return self._post(
            "Counterparty",
            "update",
            {
                "Ref": agent_ref,
                "FirstName": first_name,
                "MiddleName": middle_name,
                "LastName": last_name,
                "Phone": phone,
                "Email": email,
                "CounterpartyType": counterparty_type.value,
                "CounterpartyProperty": counterparty_property.value,
                "CityRef": city_ref,
            },
        )

    def get_sender_data(self) -> Dict[str, Any]:
        senders = self.find_agents_by_property(property=PersonProperty.SENDER)
        if not senders.get("status"):
            return senders

        out: List[Dict[str, Any]] = []
        for s in senders.get("agents", []):
            contacts = self._post("Counterparty", "getCounterpartyContactPersons", {"Ref": s["Ref"]})
            if not contacts.get("status"):
                return contacts
            c0 = (contacts.get("data") or [{}])[0]
            out.append(
                {
                    "agent_ref": s.get("Ref"),
                    "agent_description": s.get("Description"),
                    "agent_type": s.get("CounterpartyType"),
                    "agent_edrpou": s.get("EDRPOU"),
                    "agent_city_ref": s.get("City"),
                    "agent_city": s.get("CityDescription"),
                    "contact_ref": c0.get("Ref"),
                    "first_name": c0.get("FirstName"),
                    "last_name": c0.get("LastName"),
                    "middle_name": c0.get("MiddleName"),
                    "phone": c0.get("Phones"),
                    "email": c0.get("Email"),
                }
            )
        return {"status": True, "status_code": Status.OK, "senders": out, "total": senders.get("total", len(out))}

    @staticmethod
    def find_sender_by_full_name(first_name: str, last_name: str, middle_name: str, data: List[Dict[str, Any]]):
        idx = {
            (
                (d.get("first_name") or "").strip().lower(),
                (d.get("last_name") or "").strip().lower(),
                (d.get("middle_name") or "").strip().lower(),
            ): d
            for d in data
        }
        return idx.get((first_name.strip().lower(), last_name.strip().lower(), middle_name.strip().lower()))

    def is_valid_key(self) -> bool:
        res = self._post("ScanSheet", "getScanSheetList")
        return bool(res.get("status")) and res.get("status_code") == Status.OK

    @staticmethod
    def set_additional_parameters(
            sender: NovaPoshtaSender,
            receiver: NovaPoshtaReceiver,
            specified_weight: float,
            cargo_type: CargoType = CargoType.CARGO,
            backward_delivery: bool = False,
            backward_amount: str = " ",
            payer_type: str = PersonProperty.SENDER.value,
    ) -> Dict[str, Any]:
        options_seat = None
        if WarehouseType.POST_MACHINE.value in sender.address or WarehouseType.POST_MACHINE.value in receiver.address:
            cargo_type = CargoType.PARCEL
            options_seat = [
                {
                    "volumetricVolume": "1",
                    "volumetricWidth": "40",
                    "volumetricLength": "40",
                    "volumetricHeight": "30",
                    "weight": str(specified_weight),
                }
            ]
        backward_delivery_data = None
        if backward_delivery:
            backward_delivery_data = [
                {"PayerType": payer_type, "CargoType": CargoType.MONEY.value, "RedeliveryString": backward_amount}]
        return {"options_seat": options_seat, "cargo_type": cargo_type.value,
                "backward_delivery_data": backward_delivery_data}

    def create_waybill(
            self,
            sender: NovaPoshtaSender,
            receiver: NovaPoshtaReceiver,
            declared_price: str,
            delivery_date: datetime,
            specified_weight: float,
            payment_method: str = PaymentMethod.NON_CASH.value,
            payer_type: str = PersonProperty.SENDER.value,
            cargo_type: CargoType = CargoType.CARGO,
            comment: str = "",
            goods_count: int = 1,
            with_backward_delivery: bool = False,
            backward_amount: float = 0,
            postpaid_amount: float = 0,
    ):
        if not sender.max_weight_allowed or not receiver.max_weight_allowed:
            return {"status": False, "status_code": 400, "error": "Invalid max weight"}
        if specified_weight > sender.max_weight_allowed:
            return {"status": False, "status_code": 400, "error": "Too much specified weight (sender)"}
        if specified_weight > receiver.max_weight_allowed:
            return {"status": False, "status_code": 400, "error": "Too much specified weight (receiver)"}

        ap = self.set_additional_parameters(
            sender, receiver, specified_weight, cargo_type, with_backward_delivery, str(backward_amount), payer_type
        )
        props = {
            "PayerType": payer_type,
            "PaymentMethod": payment_method,
            "DateTime": delivery_date.strftime("%d.%m.%Y"),
            "CargoType": ap["cargo_type"],
            "Weight": str(specified_weight),
            "ServiceType": ServiceType.WAREHOUSE_WAREHOUSE.value,
            "SeatsAmount": goods_count,
            "Description": comment or "Доставка у відділення",
            "Cost": declared_price,
            "CitySender": sender.city_identifier,
            "Sender": sender.agent_identifier,
            "SenderAddress": sender.address_identifier,
            "ContactSender": sender.agent_contact_identifier,
            "SendersPhone": sender.phone,
            "RecipientsPhone": receiver.delivery_receiver.phone,
            "CityRecipient": receiver.city_identifier,
            "Recipient": receiver.agent_identifier,
            "RecipientAddress": receiver.address_identifier,
            "ContactRecipient": receiver.agent_contact_identifier,
            "OptionsSeat": ap["options_seat"],
        }
        if payment_method == PaymentMethod.CASH.value:
            props["BackwardDeliveryData"] = ap["backward_delivery_data"]
        else:
            props["AfterpaymentOnGoodsCost"] = str(postpaid_amount)

        return self._post("InternetDocument", "save", props)

    def delete_waybill(self, ref: str):
        return self._post("InternetDocument", "delete", {"DocumentRefs": ref})

    def update_waybill(self, nova_poshta: NovaPoshta, goods_count: int = 1):
        ap = self.set_additional_parameters(
            nova_poshta.sender,
            nova_poshta.receiver,
            nova_poshta.specified_weight,
            backward_delivery=nova_poshta.with_backward_delivery,
            backward_amount=str(nova_poshta.backward_amount),
            payer_type=nova_poshta.delivery.payer,
        )
        pm = nova_poshta.delivery.get_payment_method_display()
        props = {
            "Ref": nova_poshta.delivery_identifier,
            "PayerType": nova_poshta.delivery.payer,
            "PaymentMethod": pm,
            "DateTime": nova_poshta.delivery_date.strftime("%d.%m.%Y"),
            "CargoType": ap["cargo_type"],
            "Weight": str(nova_poshta.specified_weight),
            "ServiceType": ServiceType.WAREHOUSE_WAREHOUSE.value,
            "SeatsAmount": goods_count,
            "Description": nova_poshta.delivery.comment or "Доставка у відділення",
            "Cost": str(nova_poshta.delivery.estimate_order_price),
            "CitySender": nova_poshta.sender.city_identifier,
            "Sender": nova_poshta.sender.agent_identifier,
            "SenderAddress": nova_poshta.sender.address_identifier,
            "ContactSender": nova_poshta.sender.agent_contact_identifier,
            "SendersPhone": nova_poshta.sender.phone,
            "RecipientsPhone": nova_poshta.receiver.delivery_receiver.phone,
            "CityRecipient": nova_poshta.receiver.city_identifier,
            "Recipient": nova_poshta.receiver.agent_identifier,
            "RecipientAddress": nova_poshta.receiver.address_identifier,
            "ContactRecipient": nova_poshta.receiver.agent_contact_identifier,
            "VolumeGeneral": None,
        }
        if pm == PaymentMethod.CASH.value:
            props["BackwardDeliveryData"] = ap["backward_delivery_data"]
        else:
            props["AfterpaymentOnGoodsCost"] = str(nova_poshta.postpaid_amount)
        return self._post("InternetDocument", "update", props)

    def waybill_status(self, nova_poshta: NovaPoshta):
        res = self._post("TrackingDocument", "getStatusDocuments",
                         {"Documents": [{"DocumentNumber": nova_poshta.delivery.ttn}]})
        if not res.get("status"):
            return res
        d = (res.get("data") or [{}])[0]
        return {
            "status": True,
            "status_code": res.get("status_code", 200),
            "document": {
                "ref": d.get("RefEW"),
                "ttn": d.get("Number"),
                "status_code": int(d.get("StatusCode") or 0),
                "status_description": d.get("Status"),
                "last_scan_date": d.get("DateScan"),
                "last_tracking_date": d.get("TrackingUpdateDate"),
            },
        }

    def print_waybill_doc(self, waybill_ref: str):
        try:
            url = f"{self.MY_NP_URL}/orders/printDocument/orders[]/{waybill_ref}/type/pdf/apiKey/{self.api_key}"
            r = self.session.get(url, timeout=self.timeout)
            r.raise_for_status()
            fname = self._fname(r.headers.get("Content-Disposition", ""), f"{waybill_ref}.pdf")
            return {
                "success": True,
                "status": r.status_code,
                "document": {"filename": fname, "base64": base64.b64encode(r.content).decode("utf-8"),
                             "type": r.headers.get("Content-Type")},
            }
        except requests.HTTPError as e:
            return {"success": False, "status": getattr(e.response, "status_code", 500), "error": str(e)}
        except requests.RequestException as e:
            return {"success": False, "status": 400, "error": str(e)}
