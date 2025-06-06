from itertools import islice
from multiprocessing import Pool
from pydantic import BaseModel, UUID4, validator
from typing import List, Literal, Optional, Dict
import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import HTTPError
import json


def check_account_providers(user_id: str) -> Optional[Dict]:
    base_url = f'http://api.live.klar.internal.io/accounts/internal/v3/users/{user_id}'
    req = requests.get(
        url=base_url,
        verify=False
    )
    try:
        req.raise_for_status()
        res = req.json()
    except HTTPError:
        print(f'Error retrieving accounts for {user_id}!')
        res = None

    return res


def check_cards(parabilium_internal_id: str) -> Optional[Dict]:
    base_url = f'http://api.live.klar.internal.io/provider-parabilium/cs/cards?'
    params = {
        'accountId': parabilium_internal_id
    }
    req = requests.get(
        url=base_url,
        params=params,
        verify=False
    )
    try:
        req.raise_for_status()
        res = req.json()
    except HTTPError:
        print(f'Error retrieving cards for {parabilium_internal_id}!')
        res = None

    return res


card_targets = Literal['ACTIVE', 'FROZEN', 'FROZEN_PERMANENT']
def update_card_status(klrid: str, target_status: card_targets = 'FROZEN_PERMANENT') -> bool:
    url_ = f"http://api.live.klar.internal.io/provider-parabilium/cs/cards/{klrid}/status"

    parabilium_payload = json.dumps(
        {
            'targetStatus': target_status
        }
    )

    parabilium_headers = {
        'Content-Type': 'application/json'
    }

    req = requests.post(
        url=url_,
        headers=parabilium_headers,
        data=parabilium_payload
    )

    try:
        req.raise_for_status()
        was_blocked = True

    except HTTPError:
        print(f"Failed to change status of card {klrid} to {target_status}")
        was_blocked = False

    return was_blocked


def delete_virtual_card(klrid: str) -> bool:
    url_ = f'http://api.live.klar.internal.io/provider-parabilium/cs/cards/{klrid}'

    headers = {
        'Content-Type': 'application/json'
    }

    req = requests.delete(
        url=url_,
        headers=headers,
    )

    try:
        req.raise_for_status()
        was_deleted = True

    except HTTPError:
        print(f"Failed to delete virtual card {klrid}")
        was_deleted = False

    return was_deleted


class UserCard(BaseModel):
    user_id: Optional[UUID4]
    klrid: UUID4
    card_status: Literal[
        "ACTIVATED_NO_PIN",
        "ACTIVE",
        "BROKEN",
        "CANCELED",
        "CREATED",
        "DEACTIVATED",
        "DELETED",
        "FROZEN",
        "FROZEN_PERMANENT",
        "LOST",
        "REORDERED_KLAYUDA",
        "SHIPPED",
        "STOLEN"
    ]
    card_type: Literal[
        "PHYSICAL",
        "VIRTUAL"
    ]

    def delete(self) -> None:
        if self.card_type == 'VIRTUAL':
            was_deleted = delete_virtual_card(self.klrid)

            if was_deleted:
                self.card_status = 'CANCELED'

    def frozen_permanent(self) -> None:
        successful = update_card_status(
            klrid=self.klrid,
            target_status='FROZEN_PERMANENT'
        )
        if successful:
            self.card_status = 'FROZEN_PERMANENT'

    def frozen(self):
        successful = update_card_status(
            klrid=self.klrid,
            target_status='FROZEN'
        )
        if successful:
            self.card_status = 'FROZEN'

    def active(self):
        successful = update_card_status(
            klrid=self.klrid,
            target_status='ACTIVE'
        )
        if successful:
            self.card_status = 'ACTIVE'

    def cancel(self):
        if self.card_type == 'VIRTUAL':
            self.delete()

        else:
            self.frozen_permanent()


class UserAccount(BaseModel):
    user_id: UUID4
    parabilium_account_id: str
    active_spei: List[UUID4]
    has_mobile_access: Optional[bool]
    cards: List[UserCard]

    def check_active_services(self):
        response = {
            'user_id': self.user_id,
            'has_mobile_access': self.has_mobile_access,
            'has_active_spei': len(self.active_spei) > 0,
            'has_active_cards': False
        }

        for card in self.cards:
            if card.card_status in ['ACTIVE', 'FROZEN', 'ACTIVATED_NO_PIN', 'CREATED', 'SHIPPED']:
                response['has_active_cards'] = True
                break

        return response

    def get_active_cards(self) -> List[UserCard]:
        active_cards = []
        for card in self.cards:
            if card.card_status in ['ACTIVE', 'FROZEN', 'ACTIVATED_NO_PIN', 'CREATED', 'SHIPPED']:
                active_cards.append(card)

        return active_cards


class AccountManager:
    def __init__(self, klayuda_mail, klayuda_password):
        self.klayuda_auth = HTTPBasicAuth(klayuda_mail, klayuda_password)
        self.users: Dict[str, UserAccount] = {}

    def check_mobile_access(self, *user_id: str, batch_size: int = 10) -> Optional[Dict[str, bool]]:
        headers = {
            'Authorization': 'Basic cm9kb2xmby5kdXJhbnRlQGRpbm1heC5jb206RWxSb2RvMTY0NCE=',
            'Content-Type': 'application/json'
        }

        url_ = 'http://api.live.klar.internal.io/operation-support-service/api/customer/isEnabled'

        users = iter(user_id)
        results = {}
        while current_batch := list(islice(users, batch_size)):
            payload = json.dumps(
                current_batch
            )

            req = requests.post(
                url=url_,
                headers=headers,
                data=payload,
                auth=self.klayuda_auth
            )

            try:
                req.raise_for_status()
                for entry in req.json():
                    results[entry['customerId']] = entry['active']

            except HTTPError:
                print(f"Failed to get mobile access status for {len(user_id)} users!")
                for user in current_batch:
                    results[user] = None

        return results

    def block_mobile_access(self, user_id: str) -> bool:
        url_ = f'http://api.live.klar.internal.io/operation-support-service/api/customer/{user_id}/lock-mobile-access'

        mobile_payload = json.dumps(
            {
                "enabled": False
            }
        )

        mobile_headers = {
            'Authorization': 'Basic eWV2Z2VuaXkua29sb2tvbHRzZXZAa2xhci5teDoxMjM0NQ==',
            'Content-Type': 'application/json'
        }

        req = requests.post(
            url=url_,
            headers=mobile_headers,
            data=mobile_payload,
            auth=self.klayuda_auth,
            timeout=15
        )

        try:
            req.raise_for_status()
            response = True

        except HTTPError:
            print(f'Failed to lock mobile access for {user_id}')
            response = False

        return response

    def block_spei(self, user_id: str, spei_internal_id: str) -> bool:
        url_ = f'http://api.live.klar.internal.io/operation-support-service/api/customer/{user_id}/SPEI/{spei_internal_id}/deactivate?deactivationReason=FRAUD'
        spei_headers = {
            'Authorization': 'Basic *********************',
            'Content-Type': 'application/json'
        }
        print(url_)

        req = requests.post(
            url=url_,
            headers=spei_headers,
            auth=self.klayuda_auth,
            timeout=15
        )

        try:
            req.raise_for_status()
            response = True

        except HTTPError:
            print(f'Failed to block spei {spei_internal_id} for user {user_id}')
            response = False

        return response

    def create_user_instance(self, *user_id) -> Dict:
        mobile_access = self.check_mobile_access(*user_id)
        instances = {}

        for user in user_id:
            account_providers = check_account_providers(user_id=user)

            if account_providers is None:
                instances[user] = None
                continue

            parabilium_internal_id = 'not_found'
            spei = []
            for account in account_providers['accounts']:
                provider = account.get('providerId', None)
                if provider == 'SPEI':
                    spei_internal_id = account.get('internalId', None)
                    if spei_internal_id is not None:
                        spei.append(spei_internal_id)
                if provider == 'PARABILIUM':
                    parabilium_internal_id = account.get('internalId', None)

            if parabilium_internal_id == 'not_found':
                cards = []
            else:
                cards = check_cards(parabilium_internal_id=parabilium_internal_id)

            if cards is None:
                instances[user] = None
                continue

            user_cards = []
            for card in cards:
                current_card = UserCard(
                    user_id=user,
                    klrid=card['id'],
                    card_status=card['status'],
                    card_type=card['cardType']
                )
                user_cards.append(current_card)

            instances[user] = UserAccount(
                user_id=user,
                parabilium_account_id=parabilium_internal_id,
                active_spei=spei,
                has_mobile_access=mobile_access[user],
                cards=user_cards
            )
        return instances

    def create_user_instances(self, chunk):
        return self.create_user_instance(*chunk)

    def create_user_instance_batch(self, *user_id, batch_size: int = 50, n_process: int = 4) -> List[Dict]:
        user_id_ = iter(user_id)
        chunks = [
            list(islice(user_id_, batch_size)) for _ in range((len(user_id) + batch_size - 1) // batch_size)
        ]

        with Pool(processes=n_process) as pool_:
            results = pool_.map(
                self.create_user_instances,
                chunks
            )

        for dictionary in results:
            self.users.update(dictionary)

        return results

    def refresh_all_data(self):
        users_to_refresh = list(self.users.keys())

        if len(users_to_refresh) > 0:
            self.create_user_instance(*users_to_refresh)
        else:
            print('Nothing to do!')

    def block_mobile_user_account(self, user: UserAccount) -> None:
        user_id = user.user_id
        if user.has_mobile_access:
            was_blocked = self.block_mobile_access(user_id=user_id)
            if was_blocked:
                user.has_mobile_access = False

    def block_spei_user_account(self, user: UserAccount) -> None:
        for spei_id in user.active_spei:
            was_blocked = self.block_spei(
                user_id=user.user_id,
                spei_internal_id=spei_id
            )
            if was_blocked:
                user.active_spei.remove(spei_id)

    @staticmethod
    def block_cards_user_account(user: UserAccount) -> None:
        active_cards = user.get_active_cards()
        for card in active_cards:
            card.cancel()

    def cancel_account(self, user: UserAccount) -> None:
        self.block_mobile_user_account(user=user)
        self.block_spei_user_account(user=user)
        self.block_cards_user_account(user=user)

    def block_all(self, refresh: bool = False):
        all_users = self.users.values()
        for user in all_users:
            self.cancel_account(user=user)

        if refresh:
            self.refresh_all_data()

    def get_services_summary(self):
        summary = []
        for user in self.users.values():
            current = user.check_active_services()
            summary.append(current)

        return summary
