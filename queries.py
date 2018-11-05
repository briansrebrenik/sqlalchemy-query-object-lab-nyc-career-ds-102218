from sqlalchemy import *
from seed import Company
from sqlalchemy.orm import sessionmaker

engine = create_engine('sqlite:///dow_jones.db', echo=True)
Session = sessionmaker(bind=engine)
session = Session()

def return_apple():
    apple = session.query(Company).filter(Company.company == 'Apple').first()
    return apple
def return_disneys_industry():
    disney = session.query(Company).filter(Company.company == 'Walt Disney').first()
    return disney.industry

def return_list_of_company_objects_ordered_alphabetically_by_symbol():
    companies = session.query(Company).order_by(Company.symbol).all()
    return companies

def return_list_of_dicts_of_tech_company_names_and_their_EVs_ordered_by_EV_descending():
    tech_companies = session.query(Company).filter(Company.industry == 'Technology').order_by(Company.enterprise_value.desc())
    tech = []
    for co in tech_companies:
        obj = {'company': co.company, 'EV': co.enterprise_value}
        tech.append(obj)
    return tech

def return_list_of_consumer_products_companies_with_EV_above_225():
    companies = session.query(Company).filter(Company.industry == 'Consumer products',
    Company.enterprise_value > 225).all()
    cp_companies = []
    for co in companies:
        obj = {'name': co.company}
        cp_companies.append(obj)
    return cp_companies

def return_conglomerates_and_pharmaceutical_companies():
    all = []
    for co in session.query(Company).filter(or_(Company.industry == 'Pharmaceuticals', Company.industry == 'Conglomerate')):
        all.append(co.company)
    return all

def avg_EV_of_dow_companies():
    return session.query(func.avg(Company.enterprise_value)).first()

def return_industry_and_its_total_EV():
    return session.query(Company.industry, func.sum(Company.enterprise_value)).group_by(Company.industry).all()
