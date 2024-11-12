from fastapi import HTTPException, status, Security, Depends
from fastapi.security import OAuth2AuthorizationCodeBearer
from keycloak import KeycloakOpenID
from enacit4r_auth.models.auth import User


class KeycloakService:
    """A service to interact with keycloak for authentication and authorization.
    """

    def __init__(self, url: str, realm: str, client_id: str, client_secret: str, admin_role: str):
        # This is used for fastapi docs authentification
        self.oauth2_scheme = OAuth2AuthorizationCodeBearer(
            authorizationUrl=f"{url}",
            tokenUrl=(
                f"{url}/realms/{realm}"
                "/protocol/openid-connect/token"
            ),
        )
        self.keycloak_openid = KeycloakOpenID(
            server_url=url,
            client_id=client_id,
            client_secret_key=client_secret,
            realm_name=realm,
            verify=True,
        )
        self.admin_role = admin_role

    def get_payload(self):
        """Get the payload/token from keycloak"""
        async def get_payload_impl(token: str = Security(self.oauth2_scheme)) -> dict:
            try:
                return self.keycloak_openid.decode_token(
                    token,
                )
            except Exception as e:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail=str(e),  # "Invalid authentication credentials",
                    headers={"WWW-Authenticate": "Bearer"},
                )
        return get_payload_impl

    def get_user_info(self):
        """Get user info from the payload
        """
        async def get_user_info_impl(payload: dict = Depends(self.get_payload())) -> User:
            try:
                return User(
                    id=payload.get("sub"),
                    username=payload.get("preferred_username"),
                    email=payload.get("email"),
                    first_name=payload.get("given_name"),
                    last_name=payload.get("family_name"),
                    realm_roles=payload.get(
                        "realm_access", {}).get("roles", []),
                    client_roles=payload.get(
                        "realm_access", {}).get("roles", []),
                )
            except Exception as e:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=str(e),  # "Invalid authentication credentials",
                    headers={"WWW-Authenticate": "Bearer"},
                )
        return get_user_info_impl

    def require_admin(self):
        """Require that the user has the admin role to perform an operation
        """
        async def require_admin_impl(user: User = Depends(self.get_user_info())):
            if self.admin_role not in user.realm_roles:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You are not authorised to perform this operation",
                    headers={"WWW-Authenticate": "Bearer"},
                )
        return require_admin_impl
