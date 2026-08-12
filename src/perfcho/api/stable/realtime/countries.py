"""Map ISO alpha-2 account countries to osu! Stable's legacy byte identifiers."""

_CODE_GROUPS = (
    "OC EU AD AE AF AG AI AL AM AN AO AQ AR AS AT AU AW AZ BA BB BD BE BF BG BH BI BJ BM BN BO BR BS BT BV BW BY BZ",
    "CA CC CD CF CG CH CI CK CL CM CN CO CR CU CV CX CY CZ DE DJ DK DM DO DZ EC EE EG EH ER ES ET FI FJ FK FM FO FR",
    "FX GA GB GD GE GF GH GI GL GM GN GP GQ GR GS GT GU GW GY HK HM HN HR HT HU ID IE IL IN IO IQ IR IS IT JM JO JP",
    "KE KG KH KI KM KN KP KR KW KY KZ LA LB LC LI LK LR LS LT LU LV LY MA MC MD MG MH MK ML MM MN MO MP MQ MR MS MT MU",
    "MV MW MX MY MZ NA NC NE NF NG NI NL NO NP NR NU NZ OM PA PE PF PG PH PK PL PM PN PR PS PT PW PY QA RE RO RU RW SA",
    "SB SC SD SE SG SH SI SJ SK SL SM SN SO SR ST SV SY SZ TC TD TF TG TH TJ TK TM TN TO TL TR TT TV TW TZ UA UG UM US",
    "UY UZ VA VC VE VG VI VN VU WF WS YE YT RS ZA ZM ME ZW XX A2 O1 AX GG IM JE BL MF",
)

_COUNTRY_IDS = {code: index for index, code in enumerate(" ".join(_CODE_GROUPS).split(), start=1)}


def stable_country_id(country_code: str | None) -> int:
    """Return Stable's country byte, using its explicit unknown value 244."""
    if country_code is None:
        return 244
    return _COUNTRY_IDS.get(country_code.upper(), 244)
