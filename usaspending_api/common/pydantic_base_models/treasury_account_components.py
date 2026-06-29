from pydantic import BaseModel


class TreasuryAccountComponentsObject(BaseModel):
    aid: str
    main: str
    a: str | None = None
    ata: str | None = None
    bpoa: str | None = None
    epoa: str | None = None
    sub: str | None = None
