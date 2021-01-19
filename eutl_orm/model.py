from datetime import datetime

from sqlalchemy import (Integer, Float, Column, String, ForeignKey, Boolean, DateTime,
                        BigInteger)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref

Base = declarative_base()


class Transaction(Base):
    """ Transaction blocks """
    __tablename__ = "transaction"

    id = Column(Integer, primary_key=True, autoincrement=True)
    transactionID = Column(String(100))
    date = Column(DateTime)
    transactionTypeMain_id = Column(Integer, ForeignKey("transaction_type_main_code.id"), index=True)
    transactionTypeSupplementary_id = Column(Integer, ForeignKey("transaction_type_supplementary_code.id"), index=True)
    transferringAccount_id = Column(Integer, ForeignKey("account.id"), index=True)
    acquiringAccount_id = Column(Integer, ForeignKey("account.id"), index=True)
    unitType_id = Column(String(25), ForeignKey("unit_type.id"), index=True)
    project_id = Column(Integer(), ForeignKey("offset_project.id"), index=True)
    amount = Column(BigInteger())

    # relations
    transferringAccount = relationship("Account", 
                                    foreign_keys=[transferringAccount_id], 
                                    backref="transferringTransactions")
    acquiringAccount = relationship("Account", 
                                    foreign_keys=[acquiringAccount_id], 
                                    backref="acquiringTransactions")
    unitType = relationship("UnitType", backref="transqactions")
    project = relationship("OffsetProject", backref="transactions")
    transactionTypeMain = relationship("TransactionTypeMain", backref="transactions",)
    transactionTypeSupplementary = relationship("TransactionTypeSupplementary", 
                                                backref="transactions")

    def __repr__(self):
        return "<Transaction(%r, %r, %r, %r, %r)>" % (
            self.id, self.date, self.transferringAccount_id, 
            self.acquiringAccount_id, self.amount)


class Account(Base):
    __tablename__ = "account"

    id = Column(Integer(), primary_key=True)
    name = Column(String(250))
    registry_id = Column(String(10), ForeignKey("country_code.id"), index=True)
    accountHolder_id = Column(Integer(), ForeignKey("account_holder.id"), index=True)
    accountType_id = Column(String(10), ForeignKey("account_type_code.id"), index=True)
    isOpen = Column(Boolean())
    openingDate = Column(DateTime())
    closingDate = Column(DateTime())
    commitmentPeriod = Column(String(100))
    companyRegistrationNumber = Column(String(250))
    isRegisteredEutl = Column(Boolean(), default=True)
    installation_id = Column(String(100), ForeignKey("installation.id"), index=True)

    # relations
    accountType = relationship("AccountType", backref="accounts")
    registry = relationship("CountryCode", backref="accounts")
    installation = relationship("Installation", backref="accounts")
    accountHolder = relationship("AccountHolder", backref="accounts")

    # transferringTransactions --> all transactions with account as transferring side
    # acquiringTransactions --> all transactions with account as acquiring side

    def __repr__(self):
        return "<Account(%r, %r, %r, %r)>" % (self.id, self.name, self.registry_id, self.accountType_id)


class AccountHolder(Base):
    __tablename__ = "account_holder"
    id = Column(Integer(), primary_key=True)
    name = Column(String(300))
    addressMain = Column(String(300))
    addressSecondary = Column(String(300))
    postalCode = Column(String(300))
    city = Column(String(300))
    country_id = Column(String(300), ForeignKey("country_code.id"), index=True)

    # relations
    country = relationship("CountryCode", backref="accountHolders")

    # accounts ==> all account related to AccountHolder

    def __repr__(self):
        return "<AccountHolder(%r, %r, %r)>" % (self.id, self.name, self.country_id)


class Installation(Base):
    """EUTL regulated entity"""
    __tablename__ = "installation"

    id = Column(String(20), primary_key=True)
    name = Column(String(250))
    registry_id = Column(String(2), ForeignKey("country_code.id"), index=True)
    activity_id = Column(Integer(), ForeignKey("activity_type_code.id"), 
						 nullable=False, index=True)
    eprtrID = Column(String(200))
    parentCompany = Column(String(250))
    subsidiaryCompany = Column(String(250))
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
    entitlement = Column(Integer())

    # relationships
    registry = relationship("CountryCode", 
                            backref="installations_in_registry", 
                            foreign_keys=[registry_id])
    country = relationship("CountryCode", 
                        backref="installations_in_country", 
                        foreign_keys=[country_id])
    activityType = relationship("ActivityType", backref="installations")
    compliance = relationship("Compliance", backref="installation")
    surrendering = relationship("Surrender", backref="installation")
    nace = relationship("NaceCode", backref="installations")

    # accounts ==> all operator accounts related to installations

    def __repr__(self):
        return "<Installation(%r, %r, %r)>" % (self.id, self.name, self.registry)


class Compliance(Base):
    """compliance data"""
    __tablename__ = "compliance"

    installation_id = Column(String(100), ForeignKey("installation.id"), primary_key=True)
    year = Column(Integer(), primary_key=True)
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

    # relations
    compliance = relationship("ComplianceCode", backref="compliances")
    # installation ==> related installation

    def __repr__(self):
        return "<Compliance(%r, %r): allocated: %r, surrendered: %r, verified: %r>" % (self.installation_id, self.year,
                                                                                       self.allocatedTotal, self.surrendered,
                                                                                       self.verified)


class Surrender(Base):
    """surrendering details"""
    __tablename__ = "surrender"
    id = Column(Integer, primary_key=True)
    installation_id = Column(String(100), ForeignKey("installation.id"), index=True)
    year = Column(Integer(), index=True)
    unitType_id = Column(String(25), ForeignKey("unit_type.id"), index=True)
    amount = Column(Integer())
    originatingRegistry_id = Column(String(10), ForeignKey("country_code.id"), index=True)
    project_id = Column(Integer(), ForeignKey("offset_project.id"), index=True)

    # relations
    unitType = relationship("UnitType", backref="surrendering")
    originatingCountry = relationship("CountryCode", backref="surrendering")
    project = relationship("OffsetProject", backref="surrendering")
    # installation ==> related installation    

    def __repr__(self):
        return "<Surrendering(%r, %r)>" % (self.installation_id, self.year)


class OffsetProject(Base):
    """ ERU and CER projects """
    __tablename__ = "offset_project"
    id = Column(Integer(), primary_key=True)
    track = Column(Integer())
    country_id = Column(String(10), ForeignKey("country_code.id"))

    # relations
    country = relationship("CountryCode", backref="offsetProjects")
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
    """ Supplementary transaction type """
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


class CountryCode(Base):
    """Lookup table for countries"""
    __tablename__ = "country_code"

    id = Column(String(10), primary_key=True)
    description = Column(String(250), nullable=False)

    def __repr__(self):
        return "<CountryCode(%r, %r)>" % (self.id, self.description)
    
    # installations_in_country ==> installations located in the country
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

    childs = relationship("NaceCode",
                            backref=backref('parent', remote_side=[id]))
    #installations = relationship("Installation", foreign_keys=[nace])

    def __repr__(self):
        return "<NaceCode(%r, %r)>" % (self.id, self.description)