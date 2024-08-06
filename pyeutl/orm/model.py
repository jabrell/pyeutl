from datetime import datetime

from sqlalchemy import (
    Integer,
    Float,
    Column,
    String,
    ForeignKey,
    Boolean,
    DateTime,
    BigInteger,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref
import pandas as pd
import numpy as np
from .mappings import map_nace, map_activities

Base = declarative_base()


def format_address(x):
    """format address of given object"""
    res = ""
    if x.addressMain:
        res += x.addressMain + "\n"
    if x.addressSecondary:
        res += x.addressSecondary + "\n"
    if x.postalCode:
        res += x.postalCode + " " + x.city + "\n"
    if x.country_id:
        res += x.country.description
    if res.endswith("\n"):
        res = res[:-1]
    return res


class Transaction(Base):
    """Transaction blocks"""

    __tablename__ = "transaction"

    id = Column(Integer, primary_key=True, autoincrement=True)
    transactionID = Column(String(100))
    tradingSystem_id = Column(String(20), ForeignKey("trading_system_code.id"))
    date = Column(DateTime)
    acquiringYear = Column(Integer())
    transferringYear = Column(Integer())
    transactionTypeMain_id = Column(
        Integer, ForeignKey("transaction_type_main_code.id"), index=True
    )
    transactionTypeSupplementary_id = Column(
        Integer, ForeignKey("transaction_type_supplementary_code.id"), index=True
    )
    transferringAccount_id = Column(Integer, ForeignKey("account.id"), index=True)
    acquiringAccount_id = Column(Integer, ForeignKey("account.id"), index=True)
    unitType_id = Column(String(25), ForeignKey("unit_type.id"), index=True)
    project_id = Column(Integer(), ForeignKey("offset_project.id"), index=True)
    amount = Column(BigInteger())

    # relations
    transferringAccount = relationship(
        "Account",
        foreign_keys=[transferringAccount_id],
        backref="transferringTransactions",
    )
    acquiringAccount = relationship(
        "Account", foreign_keys=[acquiringAccount_id], backref="acquiringTransactions"
    )
    unitType = relationship("UnitType", backref="transactions")
    project = relationship("OffsetProject", backref="transactions")
    transactionTypeMain = relationship(
        "TransactionTypeMain",
        backref="transactions",
    )
    transactionTypeSupplementary = relationship(
        "TransactionTypeSupplementary", backref="transactions"
    )

    def to_dict(self):
        res = {
            k: v
            for k, v in self.__dict__.items()
            if k
            not in [
                "_sa_instance_state",
                "transferringAccount",
                "acquiringAccount",
                "unitType",
                "project",
                "transactionTypeMain",
                "transactionTypeSupplementary",
            ]
        }
        if self.unitType_id:
            res["unitType"] = self.unitType.description
        if self.transactionTypeMain_id:
            res["transactionTypeMain"] = self.transactionTypeMain.description
        if self.transactionTypeSupplementary_id:
            res["transactionTypeSupplementary"] = (
                self.transactionTypeSupplementary.description
            )
        if self.acquiringAccount_id:
            res["acquiringAccountName"] = self.acquiringAccount.name
            if self.acquiringAccount.accountType:
                res["acquiringAccountType"] = (
                    self.acquiringAccount.accountType.description
                )
            else:
                res["transferringAccountType"] = None
        if self.transferringAccount_id:
            res["transferringAccountName"] = self.transferringAccount.name
            if self.transferringAccount.accountType:
                res["transferringAccountType"] = (
                    self.transferringAccount.accountType.description
                )
            else:
                res["transferringAccountType"] = None
        return res

    def __repr__(self):
        return "<Transaction(%r, %r, %r, %r, %r)>" % (
            self.id,
            self.date,
            self.transferringAccount_id,
            self.acquiringAccount_id,
            self.amount,
        )


class Account(Base):
    __tablename__ = "account"

    id = Column(Integer(), primary_key=True)
    tradingSystem_id = Column(String(20), ForeignKey("trading_system_code.id"))
    accountIDEutl = Column(Integer)
    accountIDTransactions = Column(String(100))
    accountIDESD = Column(String(50))
    yearValid = Column(Integer())
    name = Column(String(250))
    registry_id = Column(String(10), ForeignKey("country_code.id"), index=True)
    accountHolder_id = Column(Integer(), ForeignKey("account_holder.id"), index=True)
    accountType_id = Column(String(10), ForeignKey("account_type_code.id"), index=True)
    isOpen = Column(Boolean())
    openingDate = Column(DateTime())
    closingDate = Column(DateTime())
    commitmentPeriod = Column(String(100))
    companyRegistrationNumber = Column(String(250))
    companyRegistrationNumberType = Column(String(250))
    isRegisteredEutl = Column(Boolean(), default=True)
    installation_id = Column(String(100), ForeignKey("installation.id"), index=True)
    bvdId = Column(String(100))
    created_on = Column(DateTime(), default=datetime.now)
    updated_on = Column(DateTime(), default=datetime.now, onupdate=datetime.now)

    # relations
    accountType = relationship("AccountType", backref="accounts")
    registry = relationship("Country", backref="accounts")
    installation = relationship("Installation", backref="accounts")
    accountHolder = relationship("AccountHolder", backref="accounts")

    # transferringTransactions --> all transactions with account as transferring side
    # acquiringTransactions --> all transactions with account as acquiring side
    @property
    def transactions(self):
        """all tranactions, i.e., transferring and acquiring"""
        if self.transferringTransactions:
            res = [i for i in list(self.transferringTransactions)]
            res.extend(list(self.acquiringTransactions))
            return res
        else:
            return list(self.acquiringTransactions)

    def get_transactions(self):
        lst_df = []
        if self.transferringTransactions:
            df = pd.DataFrame(
                [i.to_dict() for i in list(self.transferringTransactions)]
            )
            df["direction"] = -1
            lst_df.append(df)
        if self.acquiringTransactions:
            df = pd.DataFrame([i.to_dict() for i in list(self.acquiringTransactions)])
            df["direction"] = 1
            lst_df.append(df)
        if len(lst_df) > 0:
            df = pd.concat(lst_df)
            df["amount_directed"] = df["amount"] * df["direction"]
            return df.set_index("date").sort_index()
        return

    def to_dict(self):
        res = {
            k: v for k, v in self.__dict__.items() if k not in ["_sa_instance_state"]
        }
        if self.accountType_id:
            res["accountType"] = self.accountType.description
        return res

    def __repr__(self):
        return "<Account(%r, %r, %r, %r)>" % (
            self.id,
            self.name,
            self.registry_id,
            self.accountType_id,
        )


class AccountHolder(Base):
    __tablename__ = "account_holder"
    id = Column(Integer(), primary_key=True)
    name = Column(String(300))
    tradingSystem_id = Column(String(20), ForeignKey("trading_system_code.id"))
    addressMain = Column(String(300))
    addressSecondary = Column(String(300))
    postalCode = Column(String(300))
    city = Column(String(300))
    telephone1 = Column(String(300))
    telephone2 = Column(String(300))
    eMail = Column(String(300))
    legalEntityIdentifier = Column(String(300))
    country_id = Column(String(300), ForeignKey("country_code.id"), index=True)
    created_on = Column(DateTime(), default=datetime.now)
    updated_on = Column(DateTime(), default=datetime.now, onupdate=datetime.now)

    # relations
    country = relationship("Country", backref="accountHolders")

    # accounts ==> all account related to AccountHolder
    def to_dict(self):
        res = {
            k: v for k, v in self.__dict__.items() if k not in ["_sa_instance_state"]
        }
        if self.country_id:
            res["country"] = self.country.description
        return res

    def __repr__(self):
        return "<AccountHolder(%r, %r, %r)>" % (self.id, self.name, self.country_id)


class Installation(Base):
    """EUTL regulated entity"""

    __tablename__ = "installation"

    id = Column(String(20), primary_key=True)
    name = Column(String(250))
    tradingSystem_id = Column(String(20), ForeignKey("trading_system_code.id"))
    registry_id = Column(String(2), ForeignKey("country_code.id"), index=True)
    activity_id = Column(
        Integer(), ForeignKey("activity_type_code.id"), nullable=False, index=True
    )
    eprtrID = Column(String(200))
    parentCompany = Column(String(250))
    subsidiaryCompany = Column(String(1000))
    permitID = Column(String(250))
    designatorICAO = Column(String(250))
    monitoringID = Column(String(250))
    monitoringExpiry = Column(String())
    monitoringFirstYear = Column(String(250))
    permitDateExpiry = Column(DateTime())
    isAircraftOperator = Column(Boolean())
    ec748_2009Code = Column(String(100))
    permitDateEntry = Column(DateTime())
    addressMain = Column(String(250))
    addressSecondary = Column(String(250))
    postalCode = Column(String(250))
    city = Column(String(250))
    country_id = Column(String(25), ForeignKey("country_code.id"), index=True)
    latitudeEutl = Column(Float())
    longitudeEutl = Column(Float())
    latitudeGoogle = Column(Float())
    longitudeGoogle = Column(Float())
    nace15_id = Column(String(10))
    nace20_id = Column(String(10))
    nace_id = Column(String(10), ForeignKey("nace_code.id"), index=True)
    euEntitlement = Column(Integer())
    chEntitlement = Column(Integer())

    # relationships
    registry = relationship(
        "Country",
        back_populates="installations_in_registry",
        foreign_keys=[registry_id],
    )
    country = relationship(
        "Country", back_populates="installations_in_country", foreign_keys=[country_id]
    )
    activityType = relationship("ActivityType", backref="installations")
    compliance = relationship("Compliance", backref="installation")
    surrendering = relationship("Surrender", backref="installation")
    nace = relationship("NaceCode", backref="installations")
    # accounts ==> all operator accounts related to installations
    # for shipping companies
    # todo add country relation
    isMaritimeOperator = Column(Boolean())
    shippingCompanyCountry = Column(String(10))
    shippingCompanyType = Column(String(100))
    shippingCompany = Column(String(100))
    imoID = Column(String(50))
    region = Column(String(50))

    @property
    def activity(self):
        return self.activityType.description

    @property
    def address(self):
        return format_address(self)

    @property
    def nace_category(self):
        return map_nace.get(self.nace_id)

    @property
    def activity_category(self):
        return map_activities.get(self.activity_id)

    def get_compliance(self):
        """Returns compliance data as dataframe"""
        return pd.DataFrame([c.to_dict() for c in self.compliance]).replace(
            "None", np.nan
        )

    def get_surrendering(self):
        return pd.DataFrame([s.to_dict() for s in self.surrendering]).replace(
            "None", np.nan
        )

    def to_dict(self):
        res = {
            k: v
            for k, v in self.__dict__.items()
            if k
            not in [
                "_sa_instance_state",
            ]
        }
        if self.activity_id:
            res["activity"] = self.activityType.description
            res["activity_category"] = map_activities.get(self.activity_id, np.nan)
        if self.nace_id:
            res["nace"] = self.nace.description
            res["nace_category"] = map_nace.get(self.nace_id, np.nan)
        if self.registry_id:
            res["registry"] = self.registry.description
        if self.country_id:
            res["country"] = self.country.description
        return res

    def __repr__(self):
        return "<Installation(%r, %r, %r)>" % (self.id, self.name, self.registry)


class Compliance(Base):
    """compliance data"""

    __tablename__ = "compliance"

    installation_id = Column(
        String(100), ForeignKey("installation.id"), primary_key=True
    )
    year = Column(Integer(), primary_key=True)
    reportedInSystem_id = Column(
        String(20), ForeignKey("trading_system_code.id"), primary_key=True
    )
    euetsPhase = Column(String(100))
    compliance_id = Column(String(100), ForeignKey("compliance_code.id"))
    allocatedFree = Column(Integer())
    allocatedNewEntrance = Column(Integer())
    allocatedTotal = Column(Integer())
    allocated10c = Column(Integer())
    verified = Column(Integer())
    verifiedCummulative = Column(Integer())
    verifiedUpdated = Column(Boolean())
    surrendered = Column(Integer())
    surrenderedCummulative = Column(Integer())
    balance = Column(Integer())
    penalty = Column(Integer())

    # relations
    compliance = relationship("ComplianceCode", backref="compliances")
    # installation ==> related installation

    def to_dict(self):
        res = {
            k: v for k, v in self.__dict__.items() if k not in ["_sa_instance_state"]
        }
        if self.compliance:
            res["compliance"] = self.compliance.description
        return res

    def __repr__(self):
        return "<Compliance(%r, %r): allocated: %r, surrendered: %r, verified: %r>" % (
            self.installation_id,
            self.year,
            self.allocatedTotal,
            self.surrendered,
            self.verified,
        )


class Surrender(Base):
    """surrendering details"""

    __tablename__ = "surrender"
    id = Column(Integer, primary_key=True)
    installation_id = Column(String(100), ForeignKey("installation.id"), index=True)
    reportedInSystem_id = Column(String(20), ForeignKey("trading_system_code.id"))
    year = Column(Integer(), index=True)
    unitType_id = Column(String(25), ForeignKey("unit_type.id"), index=True)
    amount = Column(Integer())
    originatingRegistry_id = Column(
        String(10), ForeignKey("country_code.id"), index=True
    )
    project_id = Column(Integer(), ForeignKey("offset_project.id"), index=True)

    # relations
    unitType = relationship("UnitType", backref="surrendering")
    originatingCountry = relationship("Country", backref="surrendering")
    project = relationship("OffsetProject", backref="surrendering")
    # installation ==> related installation

    def to_dict(self):
        res = {
            k: v
            for k, v in self.__dict__.items()
            if k not in ["_sa_instance_state", "id"]
        }
        if self.unitType:
            res["unitType"] = self.unitType.description
        else:
            res["unitType"] = None
        return res

    def __repr__(self):
        return "<Surrendering(%r, %r)>" % (self.installation_id, self.year)


class OffsetProject(Base):
    """ERU and CER projects"""

    __tablename__ = "offset_project"
    id = Column(Integer(), primary_key=True)
    track = Column(Integer())
    country_id = Column(String(10), ForeignKey("country_code.id"))

    # relations
    country = relationship("Country", backref="offsetProjects")
    # transactions ===> related transactions
    # surrendering ==> usage in surrendering data

    def __repr__(self):
        return "<OffsetProject(%r, %r, %r)>" % (self.id, self.track, self.country_id)


class TransactionTypeMain(Base):
    """Lookup table for main transaction type"""

    __tablename__ = "transaction_type_main_code"

    id = Column(Integer, primary_key=True)
    description = Column(String(250), nullable=False, index=True)

    # transactions ==> transaction with respective main code

    def __repr__(self):
        return "<TransactionTypeMain(%r, %r)>" % (self.id, self.description)


class TransactionTypeSupplementary(Base):
    """Supplementary transaction type"""

    __tablename__ = "transaction_type_supplementary_code"

    id = Column(Integer, primary_key=True)
    description = Column(String(250), nullable=False, index=True)

    # transactions ==> transaction with respective supplementary code

    def __repr__(self):
        return "<TransactionTypeSupplementary(%r, %r)>" % (self.id, self.description)


class AccountType(Base):
    """look-up table for account types"""

    __tablename__ = "account_type_code"

    id = Column(String(10), primary_key=True)
    description = Column(String(250), nullable=False, index=True)

    # accounts ==> accounts of respective type

    def __repr__(self):
        return "<AccountType(%r, %r)>" % (self.id, self.description)


class ActivityType(Base):
    """Lookup table for account types"""

    __tablename__ = "activity_type_code"

    id = Column(Integer(), primary_key=True)
    description = Column(String(250), nullable=False)

    # installations ==> installations of respective activity

    def __repr__(self):
        return "<ActivityType(%r, %r)>" % (self.id, self.description)


class UnitType(Base):
    """Lookup table for allowances unit types"""

    __tablename__ = "unit_type"

    id = Column(String(25), primary_key=True)
    description = Column(String(250), nullable=False)

    # transactions ==> transactions with respective unit type involved
    # surrendering ==> surrendering with respective unit types

    def __repr__(self):
        return "<UnitType(%r, %r)>" % (self.id, self.description)


class Country(Base):
    """Lookup table for countries"""

    __tablename__ = "country_code"

    id = Column(String(10), primary_key=True)
    description = Column(String(250), nullable=False)

    installations_in_registry = relationship(
        Installation,
        back_populates="registry",
        lazy="dynamic",
        foreign_keys=[Installation.registry_id],
    )

    installations_in_country = relationship(
        Installation,
        back_populates="country",
        lazy="dynamic",
        foreign_keys=[Installation.country_id],
    )

    @property
    def name(self):
        return self.description

    def _filter_installations(self, filter={}, origin="registry"):
        """Filter installation list
        :param filter: <dict: attribute -> list> to filter based on installation attributes
        :param origin: <string> registries for installations in registry,
                                country for installations located in the country
        return: <sqlalchemy.orm.dynamic.AppenderQuery>"""
        # get installations
        if origin == "registry":
            inst = self.installations_in_registry
        elif origin == "country":
            inst = self.installations_in_country
        else:
            raise ValueError(
                "Invalid installations origin. Has to be either 'registry' or 'country'"
            )
        # filter installations
        for k, v in filter.items():
            try:
                inst = inst.filter(getattr(Installation, k).in_(v))
            except AttributeError as e:
                raise AttributeError("Error in filter: %s" % str(e))
        return inst

    def get_compliance(self, filter={}, origin="registry"):
        """Get compliance data of registry
        :param filter: <dict: attribute -> list> to filter based on installation attributes
        :param origin: <string> registries for installations in registry,
                                country for installations located in the country
        """
        qry = self._filter_installations(filter, origin)
        qry = (
            qry.join(ActivityType, isouter=True)
            .join(NaceCode, isouter=True)
            .join(Compliance, isouter=True)
        )
        qry = qry.with_entities(
            Installation.id.label("installation_id"),
            Installation.name.label("installation_name"),
            ActivityType.id.label("activity_id"),
            ActivityType.description.label("activity"),
            NaceCode.id.label("nace_id"),
            NaceCode.description.label("nace"),
            Compliance.year,
            Compliance.surrendered,
            Compliance.verified,
            Compliance.allocatedTotal,
            Compliance.allocatedFree,
            Compliance.allocated10c,
            Compliance.allocatedNewEntrance,
        )
        df = pd.read_sql(qry.statement, qry.session.bind)
        df["nace_category"] = df.nace_id.map(lambda x: map_nace.get(x, "not provided"))
        df["activity_category"] = df.activity_id.map(
            lambda x: map_activities.get(x, "not provided")
        )
        return df

    def get_installations(self, filter={}, origin="registry"):
        """Return pandas dataframe with installations
        :param filter: <dict: attribute -> list> to filter based on installation attributes
        :param origin: <string> registries for installations in registry,
                                country for installations located in the country
        :return: <pd.DataFrame>"""
        inst = self._filter_installations(filter, origin)
        return pd.DataFrame([c.to_dict() for c in inst]).replace("None", np.nan)

    def __repr__(self):
        return "<Country(%r, %r)>" % (self.id, self.description)

    # installations_in_registry ==> installations registered in the country
    # surrendering ==> surrendering of permit originating in the country
    # offsetProject ==> offset project taking place in the country
    # accounts ==> accounts registered in the country
    # accountHolders ==> account holders with address in the country


class ComplianceCode(Base):
    """Lookup table for compliance status"""

    __tablename__ = "compliance_code"

    id = Column(String(10), primary_key=True)
    description = Column(String(250), nullable=False)

    # compliances ==> compliance with respective code

    def __repr__(self):
        return "<ComplianceCode(%r, %r)>" % (self.id, self.description)


class NaceCode(Base):
    __tablename__ = "nace_code"
    id = Column(String(10), primary_key=True)
    parent_id = Column(String(10), ForeignKey("nace_code.id"))
    level = Column(Integer)
    description = Column(String(50000))
    includes = Column(String(50000))
    includesAlso = Column(String(50000))
    ruling = Column(String(50000))
    excludes = Column(String(50000))
    isic4_id = Column(String(10))

    childs = relationship("NaceCode", backref=backref("parent", remote_side=[id]))
    # installations = relationship("Installation", foreign_keys=[nace])

    def __repr__(self):
        return "<NaceCode(%r, %r)>" % (self.id, self.description)


class TradingSystemCode(Base):
    __tablename__ = "trading_system_code"
    id = Column(String(20), primary_key=True)
    description = Column(String(250))
