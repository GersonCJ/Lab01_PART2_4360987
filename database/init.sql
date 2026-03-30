--
-- PostgreSQL database dump
--

-- Dumped from database version 17.5
-- Dumped by pg_dump version 17.5

-- Started on 2026-03-26 11:03:53

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 6 (class 2615 OID 246064)
-- Name: co2_project; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA co2_project;


SET default_table_access_method = heap;

--
-- TOC entry 225 (class 1259 OID 246123)
-- Name: agg_climate_impact; Type: TABLE; Schema: co2_project; Owner: -
--

CREATE TABLE co2_project.agg_climate_impact (
    country character varying(255) NOT NULL,
    year integer NOT NULL,
    iso_code character varying(30),
    population_people bigint,
    share_of_temperature_change_from_ghg_prct double precision,
    temperature_change_from_ch4_degrees_c double precision,
    temperature_change_from_co2_degrees_c double precision,
    temperature_change_from_ghg_degrees_c double precision,
    temperature_change_from_n2o_degrees_c double precision
);


--
-- TOC entry 4847 (class 0 OID 0)
-- Dependencies: 225
-- Name: COLUMN agg_climate_impact.country; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_climate_impact.country IS 'Country';


--
-- TOC entry 4848 (class 0 OID 0)
-- Dependencies: 225
-- Name: COLUMN agg_climate_impact.year; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_climate_impact.year IS 'Year';


--
-- TOC entry 4849 (class 0 OID 0)
-- Dependencies: 225
-- Name: COLUMN agg_climate_impact.iso_code; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_climate_impact.iso_code IS 'ISO code';


--
-- TOC entry 4850 (class 0 OID 0)
-- Dependencies: 225
-- Name: COLUMN agg_climate_impact.share_of_temperature_change_from_ghg_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_climate_impact.share_of_temperature_change_from_ghg_prct IS 'Share of contribution to global warming (prct)';


--
-- TOC entry 4851 (class 0 OID 0)
-- Dependencies: 225
-- Name: COLUMN agg_climate_impact.temperature_change_from_ch4_degrees_c; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_climate_impact.temperature_change_from_ch4_degrees_c IS 'Change in global mean surface temperature caused by methane emissions (degrees C)';


--
-- TOC entry 4852 (class 0 OID 0)
-- Dependencies: 225
-- Name: COLUMN agg_climate_impact.temperature_change_from_co2_degrees_c; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_climate_impact.temperature_change_from_co2_degrees_c IS 'Change in global mean surface temperature caused by CO₂ emissions (degrees C)';


--
-- TOC entry 4853 (class 0 OID 0)
-- Dependencies: 225
-- Name: COLUMN agg_climate_impact.temperature_change_from_ghg_degrees_c; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_climate_impact.temperature_change_from_ghg_degrees_c IS 'Change in global mean surface temperature caused by greenhouse gas emissions (degrees C)';


--
-- TOC entry 4854 (class 0 OID 0)
-- Dependencies: 225
-- Name: COLUMN agg_climate_impact.temperature_change_from_n2o_degrees_c; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_climate_impact.temperature_change_from_n2o_degrees_c IS 'Change in global mean surface temperature caused by nitrous oxide emissions (degrees C)';


--
-- TOC entry 224 (class 1259 OID 246118)
-- Name: agg_consumption; Type: TABLE; Schema: co2_project; Owner: -
--

CREATE TABLE co2_project.agg_consumption (
    country character varying(255) NOT NULL,
    year integer NOT NULL,
    iso_code character varying(30),
    population_people bigint,
    gdp_usd double precision,
    consumption_co2_mt double precision,
    consumption_co2_per_capita_t_per_person double precision,
    consumption_co2_per_gdp_kg_per_usd double precision,
    energy_per_capita_kwh double precision,
    energy_per_gdp_kwh double precision,
    primary_energy_consumption_twh double precision
);


--
-- TOC entry 4855 (class 0 OID 0)
-- Dependencies: 224
-- Name: COLUMN agg_consumption.country; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_consumption.country IS 'Country';


--
-- TOC entry 4856 (class 0 OID 0)
-- Dependencies: 224
-- Name: COLUMN agg_consumption.year; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_consumption.year IS 'Year';


--
-- TOC entry 4857 (class 0 OID 0)
-- Dependencies: 224
-- Name: COLUMN agg_consumption.iso_code; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_consumption.iso_code IS 'ISO code';


--
-- TOC entry 4858 (class 0 OID 0)
-- Dependencies: 224
-- Name: COLUMN agg_consumption.population_people; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_consumption.population_people IS 'Population';


--
-- TOC entry 4859 (class 0 OID 0)
-- Dependencies: 224
-- Name: COLUMN agg_consumption.gdp_usd; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_consumption.gdp_usd IS 'Gross domestic product (GDP)';


--
-- TOC entry 4860 (class 0 OID 0)
-- Dependencies: 224
-- Name: COLUMN agg_consumption.consumption_co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_consumption.consumption_co2_mt IS 'Annual consumption-based CO₂ emissions (measured in million tonnes)';


--
-- TOC entry 4861 (class 0 OID 0)
-- Dependencies: 224
-- Name: COLUMN agg_consumption.consumption_co2_per_capita_t_per_person; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_consumption.consumption_co2_per_capita_t_per_person IS 'Per capita consumption-based CO₂ emissions';


--
-- TOC entry 4862 (class 0 OID 0)
-- Dependencies: 224
-- Name: COLUMN agg_consumption.consumption_co2_per_gdp_kg_per_usd; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_consumption.consumption_co2_per_gdp_kg_per_usd IS 'Annual consumption-based CO₂ emissions per GDP (measured in kilograms per dollar of GDP)';


--
-- TOC entry 4863 (class 0 OID 0)
-- Dependencies: 224
-- Name: COLUMN agg_consumption.energy_per_capita_kwh; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_consumption.energy_per_capita_kwh IS 'Primary energy consumption per capita (Measured in kilowatt-hours per person)';


--
-- TOC entry 4864 (class 0 OID 0)
-- Dependencies: 224
-- Name: COLUMN agg_consumption.energy_per_gdp_kwh; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_consumption.energy_per_gdp_kwh IS 'Primary energy consumption per GDP (Measured in kilowatt-hours per USD)';


--
-- TOC entry 4865 (class 0 OID 0)
-- Dependencies: 224
-- Name: COLUMN agg_consumption.primary_energy_consumption_twh; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_consumption.primary_energy_consumption_twh IS '	Primary energy consumption (Measured in terawatt-hours)';


--
-- TOC entry 227 (class 1259 OID 246133)
-- Name: agg_emission_sources; Type: TABLE; Schema: co2_project; Owner: -
--

CREATE TABLE co2_project.agg_emission_sources (
    country character varying(255) NOT NULL,
    year integer NOT NULL,
    iso_code character varying(30),
    population_people bigint,
    gdp_usd double precision,
    cement_co2_mt double precision,
    cement_co2_per_capita_t_per_person double precision,
    coal_co2_mt double precision,
    coal_co2_per_capita_t_per_person double precision,
    cumulative_cement_co2_mt double precision,
    cumulative_coal_co2_mt double precision,
    cumulative_flaring_co2_mt double precision,
    cumulative_gas_co2_mt double precision,
    cumulative_luc_co2_mt double precision,
    cumulative_oil_co2_mt double precision,
    cumulative_other_co2_mt double precision,
    flaring_co2_mt double precision,
    flaring_co2_per_capita_t_per_person double precision,
    gas_co2_mt double precision,
    gas_co2_per_capita_t_per_person double precision,
    land_use_change_co2_mt double precision,
    land_use_change_co2_per_capita_t_per_person double precision,
    oil_co2_mt double precision,
    oil_co2_per_capita_t_per_person double precision,
    other_co2_per_capita_t_per_person double precision,
    other_industry_co2_mt double precision,
    share_global_cement_co2_prct double precision,
    share_global_coal_co2_prct double precision,
    share_global_cumulative_cement_co2_prct double precision,
    share_global_cumulative_coal_co2_prct double precision,
    share_global_cumulative_flaring_co2_prct double precision,
    share_global_cumulative_gas_co2_prct double precision,
    share_global_cumulative_luc_co2_prct double precision,
    share_global_cumulative_oil_co2_prct double precision,
    share_global_cumulative_other_co2_prct double precision,
    share_global_flaring_co2_prct double precision,
    share_global_gas_co2_prct double precision,
    share_global_luc_co2_prct double precision,
    share_global_oil_co2_prct double precision,
    share_global_other_co2_prct double precision,
    trade_co2_mt double precision,
    trade_co2_share_prct double precision
);


--
-- TOC entry 4866 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.country; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.country IS 'Country';


--
-- TOC entry 4867 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.year; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.year IS 'Year';


--
-- TOC entry 4868 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.iso_code; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.iso_code IS 'ISO code';


--
-- TOC entry 4869 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.population_people; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.population_people IS 'Population';


--
-- TOC entry 4870 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.gdp_usd; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.gdp_usd IS 'Gross domestic product (GDP)';


--
-- TOC entry 4871 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.cement_co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.cement_co2_mt IS 'Annual CO₂ emissions from cement (measured in million tonnes)';


--
-- TOC entry 4872 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.cement_co2_per_capita_t_per_person; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.cement_co2_per_capita_t_per_person IS '	Annual CO₂ emissions from cement (per capita)';


--
-- TOC entry 4873 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.coal_co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.coal_co2_mt IS 'Annual CO₂ emissions from coal (measured in million tonnes)';


--
-- TOC entry 4874 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.coal_co2_per_capita_t_per_person; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.coal_co2_per_capita_t_per_person IS 'Annual CO₂ emissions from coal (per capita)';


--
-- TOC entry 4875 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.cumulative_cement_co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.cumulative_cement_co2_mt IS 'Cumulative CO₂ emissions from cement (measured in million tonnes)';


--
-- TOC entry 4876 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.cumulative_coal_co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.cumulative_coal_co2_mt IS 'Cumulative CO₂ emissions from coal (measured in million tonnes)';


--
-- TOC entry 4877 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.cumulative_flaring_co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.cumulative_flaring_co2_mt IS 'Cumulative CO₂ emissions from flaring (measured in million tonnes)';


--
-- TOC entry 4878 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.cumulative_gas_co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.cumulative_gas_co2_mt IS 'Cumulative CO₂ emissions from gas (measured in million tonnes)';


--
-- TOC entry 4879 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.cumulative_luc_co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.cumulative_luc_co2_mt IS 'Cumulative CO₂ emissions from land-use change (measured in million tonnes)';


--
-- TOC entry 4880 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.cumulative_oil_co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.cumulative_oil_co2_mt IS 'Cumulative CO₂ emissions from oil (measured in million tonnes)';


--
-- TOC entry 4881 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.cumulative_other_co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.cumulative_other_co2_mt IS 'Cumulative CO₂ emissions from other industry (measured in million tonnes)';


--
-- TOC entry 4882 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.flaring_co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.flaring_co2_mt IS 'Annual CO₂ emissions from flaring (measured in million tonnes)';


--
-- TOC entry 4883 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.flaring_co2_per_capita_t_per_person; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.flaring_co2_per_capita_t_per_person IS 'Annual CO₂ emissions from flaring (per capita) (measured in tonnes per person)';


--
-- TOC entry 4884 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.gas_co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.gas_co2_mt IS 'Annual CO₂ emissions from gas (measured in million tonnes)';


--
-- TOC entry 4885 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.gas_co2_per_capita_t_per_person; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.gas_co2_per_capita_t_per_person IS 'Annual CO₂ emissions from gas (per capita) (measured in tonnes per person)';


--
-- TOC entry 4886 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.land_use_change_co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.land_use_change_co2_mt IS 'Annual CO₂ emissions from land-use change (measured in million tonnes)';


--
-- TOC entry 4887 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.land_use_change_co2_per_capita_t_per_person; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.land_use_change_co2_per_capita_t_per_person IS 'Annual CO₂ emissions from land-use change per capita (measured in tonnes per person)';


--
-- TOC entry 4888 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.oil_co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.oil_co2_mt IS 'Annual CO₂ emissions from oil (measured in million tonnes)';


--
-- TOC entry 4889 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.oil_co2_per_capita_t_per_person; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.oil_co2_per_capita_t_per_person IS 'Annual CO₂ emissions from oil (per capita) (measured in tonnes per person)';


--
-- TOC entry 4890 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.other_co2_per_capita_t_per_person; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.other_co2_per_capita_t_per_person IS 'Annual CO₂ emissions from other industry (per capita) (measured in tonnes per person)';


--
-- TOC entry 4891 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.other_industry_co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.other_industry_co2_mt IS 'Annual CO₂ emissions from other industry (measured in million tonnes)';


--
-- TOC entry 4892 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.share_global_cement_co2_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.share_global_cement_co2_prct IS 'Share of global annual CO₂ emissions from cement (prct)';


--
-- TOC entry 4893 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.share_global_coal_co2_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.share_global_coal_co2_prct IS 'Share of global annual CO₂ emissions (prct)';


--
-- TOC entry 4894 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.share_global_cumulative_cement_co2_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.share_global_cumulative_cement_co2_prct IS 'Share of global cumulative CO₂ emissions from cement (prct)';


--
-- TOC entry 4895 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.share_global_cumulative_coal_co2_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.share_global_cumulative_coal_co2_prct IS 'Share of global cumulative CO₂ emissions from coal (prct)';


--
-- TOC entry 4896 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.share_global_cumulative_flaring_co2_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.share_global_cumulative_flaring_co2_prct IS 'Share of global cumulative CO₂ emissions from flaring (prct)';


--
-- TOC entry 4897 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.share_global_cumulative_gas_co2_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.share_global_cumulative_gas_co2_prct IS 'Share of global cumulative CO₂ emissions from gas (prct)';


--
-- TOC entry 4898 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.share_global_cumulative_luc_co2_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.share_global_cumulative_luc_co2_prct IS 'Share of global cumulative CO₂ emissions from land-use change (prct)';


--
-- TOC entry 4899 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.share_global_cumulative_oil_co2_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.share_global_cumulative_oil_co2_prct IS 'Share of global cumulative CO₂ emissions from oil (prct)';


--
-- TOC entry 4900 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.share_global_cumulative_other_co2_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.share_global_cumulative_other_co2_prct IS '	Share of global cumulative CO₂ emissions from other industry (prct)';


--
-- TOC entry 4901 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.share_global_flaring_co2_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.share_global_flaring_co2_prct IS 'Share of global annual CO₂ emissions from flaring (prct)';


--
-- TOC entry 4902 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.share_global_gas_co2_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.share_global_gas_co2_prct IS 'Share of global annual CO₂ emissions from gas (prct)';


--
-- TOC entry 4903 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.share_global_luc_co2_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.share_global_luc_co2_prct IS 'Share of global annual CO₂ emissions from land-use change (prct)';


--
-- TOC entry 4904 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.share_global_oil_co2_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.share_global_oil_co2_prct IS 'Share of global annual CO₂ emissions from oil (prct)';


--
-- TOC entry 4905 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.share_global_other_co2_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.share_global_other_co2_prct IS 'Share of global annual CO₂ emissions from other industry (prct)';


--
-- TOC entry 4906 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.trade_co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.trade_co2_mt IS 'Annual CO₂ emissions embedded in trade (measured in million tonnes)';


--
-- TOC entry 4907 (class 0 OID 0)
-- Dependencies: 227
-- Name: COLUMN agg_emission_sources.trade_co2_share_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emission_sources.trade_co2_share_prct IS 'Share of annual CO₂ emissions embedded in trade (prct)';


--
-- TOC entry 223 (class 1259 OID 246113)
-- Name: agg_emissions; Type: TABLE; Schema: co2_project; Owner: -
--

CREATE TABLE co2_project.agg_emissions (
    country character varying(255) NOT NULL,
    year integer NOT NULL,
    iso_code character varying(30),
    population_people bigint,
    gdp_usd double precision,
    co2_mt double precision,
    co2_growth_abs_mt double precision,
    co2_growth_prct_prct double precision,
    co2_including_luc_mt double precision,
    co2_including_luc_growth_abs_mt double precision,
    co2_including_luc_growth_prct_prct double precision,
    co2_including_luc_per_capita_t_per_person double precision,
    co2_including_luc_per_gdp_kg_per_usd double precision,
    co2_including_luc_per_unit_energy_kg_per_kwh double precision,
    cumulative_co2_mt double precision,
    cumulative_co2_including_luc_mt double precision,
    co2_per_capita_t_per_person double precision,
    co2_per_gdp_kg_per_usd double precision,
    co2_per_unit_energy_kg_per_kwh double precision,
    share_global_co2_prct double precision,
    share_global_co2_including_luc_prct double precision,
    share_global_cumulative_co2_prct double precision,
    share_global_cumulative_co2_including_luc_prct double precision
);


--
-- TOC entry 4908 (class 0 OID 0)
-- Dependencies: 223
-- Name: COLUMN agg_emissions.country; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emissions.country IS 'Country';


--
-- TOC entry 4909 (class 0 OID 0)
-- Dependencies: 223
-- Name: COLUMN agg_emissions.year; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emissions.year IS 'Year';


--
-- TOC entry 4910 (class 0 OID 0)
-- Dependencies: 223
-- Name: COLUMN agg_emissions.iso_code; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emissions.iso_code IS 'ISO code';


--
-- TOC entry 4911 (class 0 OID 0)
-- Dependencies: 223
-- Name: COLUMN agg_emissions.population_people; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emissions.population_people IS 'Population';


--
-- TOC entry 4912 (class 0 OID 0)
-- Dependencies: 223
-- Name: COLUMN agg_emissions.gdp_usd; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emissions.gdp_usd IS 'Gross domestic product (GDP)';


--
-- TOC entry 4913 (class 0 OID 0)
-- Dependencies: 223
-- Name: COLUMN agg_emissions.co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emissions.co2_mt IS 'Annual CO₂ emissions (measured in million tonnes)';


--
-- TOC entry 4914 (class 0 OID 0)
-- Dependencies: 223
-- Name: COLUMN agg_emissions.co2_growth_abs_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emissions.co2_growth_abs_mt IS 'Annual CO₂ emissions growth (abs) (measured in million tonnes)';


--
-- TOC entry 4915 (class 0 OID 0)
-- Dependencies: 223
-- Name: COLUMN agg_emissions.co2_growth_prct_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emissions.co2_growth_prct_prct IS 'Annual CO₂ emissions growth (prct)';


--
-- TOC entry 4916 (class 0 OID 0)
-- Dependencies: 223
-- Name: COLUMN agg_emissions.co2_including_luc_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emissions.co2_including_luc_mt IS 'Annual CO₂ emissions including land-use change (measured in million tonnes)';


--
-- TOC entry 4917 (class 0 OID 0)
-- Dependencies: 223
-- Name: COLUMN agg_emissions.co2_including_luc_growth_abs_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emissions.co2_including_luc_growth_abs_mt IS 'Growth rate of emissions including land-use change (measured in million tonnes)';


--
-- TOC entry 4918 (class 0 OID 0)
-- Dependencies: 223
-- Name: COLUMN agg_emissions.co2_including_luc_growth_prct_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emissions.co2_including_luc_growth_prct_prct IS 'Growth rate of emissions including land-use change (prct)';


--
-- TOC entry 4919 (class 0 OID 0)
-- Dependencies: 223
-- Name: COLUMN agg_emissions.co2_including_luc_per_capita_t_per_person; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emissions.co2_including_luc_per_capita_t_per_person IS 'Annual CO₂ emissions including land-use change per capita';


--
-- TOC entry 4920 (class 0 OID 0)
-- Dependencies: 223
-- Name: COLUMN agg_emissions.co2_including_luc_per_gdp_kg_per_usd; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emissions.co2_including_luc_per_gdp_kg_per_usd IS 'Annual CO₂ emissions including land-use change per GDP';


--
-- TOC entry 4921 (class 0 OID 0)
-- Dependencies: 223
-- Name: COLUMN agg_emissions.co2_including_luc_per_unit_energy_kg_per_kwh; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emissions.co2_including_luc_per_unit_energy_kg_per_kwh IS 'Annual CO₂ emissions including land-use change per unit energy';


--
-- TOC entry 4922 (class 0 OID 0)
-- Dependencies: 223
-- Name: COLUMN agg_emissions.cumulative_co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emissions.cumulative_co2_mt IS 'Cumulative CO₂ emissions (measured in million tonnes)';


--
-- TOC entry 4923 (class 0 OID 0)
-- Dependencies: 223
-- Name: COLUMN agg_emissions.cumulative_co2_including_luc_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emissions.cumulative_co2_including_luc_mt IS 'Cumulative CO₂ emissions including land-use change (measured in million tonnes)';


--
-- TOC entry 4924 (class 0 OID 0)
-- Dependencies: 223
-- Name: COLUMN agg_emissions.co2_per_capita_t_per_person; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emissions.co2_per_capita_t_per_person IS 'CO₂ emissions per capita';


--
-- TOC entry 4925 (class 0 OID 0)
-- Dependencies: 223
-- Name: COLUMN agg_emissions.co2_per_gdp_kg_per_usd; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emissions.co2_per_gdp_kg_per_usd IS 'Annual CO₂ emissions per GDP ';


--
-- TOC entry 4926 (class 0 OID 0)
-- Dependencies: 223
-- Name: COLUMN agg_emissions.co2_per_unit_energy_kg_per_kwh; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emissions.co2_per_unit_energy_kg_per_kwh IS 'Annual CO₂ emissions per unit energy';


--
-- TOC entry 4927 (class 0 OID 0)
-- Dependencies: 223
-- Name: COLUMN agg_emissions.share_global_co2_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emissions.share_global_co2_prct IS 'Share of global annual CO₂ emissions (prct)';


--
-- TOC entry 4928 (class 0 OID 0)
-- Dependencies: 223
-- Name: COLUMN agg_emissions.share_global_co2_including_luc_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emissions.share_global_co2_including_luc_prct IS 'Share of global annual CO₂ emissions including land-use change (prct)';


--
-- TOC entry 4929 (class 0 OID 0)
-- Dependencies: 223
-- Name: COLUMN agg_emissions.share_global_cumulative_co2_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emissions.share_global_cumulative_co2_prct IS 'Share of global cumulative CO₂ emissions (prct)';


--
-- TOC entry 4930 (class 0 OID 0)
-- Dependencies: 223
-- Name: COLUMN agg_emissions.share_global_cumulative_co2_including_luc_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_emissions.share_global_cumulative_co2_including_luc_prct IS 'Share of global cumulative CO₂ emissions including land-use change (prct)';


--
-- TOC entry 226 (class 1259 OID 246128)
-- Name: agg_non_co2_ghg; Type: TABLE; Schema: co2_project; Owner: -
--

CREATE TABLE co2_project.agg_non_co2_ghg (
    country character varying(255) NOT NULL,
    year integer NOT NULL,
    iso_code character varying(30),
    population_people bigint,
    gdp_usd double precision,
    ghg_excluding_lucf_per_capita_t_per_person double precision,
    ghg_per_capita_t_per_person double precision,
    methane_mt double precision,
    methane_per_capita_t_per_person double precision,
    nitrous_oxide_mt double precision,
    nitrous_oxide_per_capita_t_per_person double precision,
    total_ghg_mt double precision,
    total_ghg_excluding_lucf_mt double precision
);


--
-- TOC entry 4931 (class 0 OID 0)
-- Dependencies: 226
-- Name: COLUMN agg_non_co2_ghg.country; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_non_co2_ghg.country IS 'Country';


--
-- TOC entry 4932 (class 0 OID 0)
-- Dependencies: 226
-- Name: COLUMN agg_non_co2_ghg.year; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_non_co2_ghg.year IS 'Year';


--
-- TOC entry 4933 (class 0 OID 0)
-- Dependencies: 226
-- Name: COLUMN agg_non_co2_ghg.iso_code; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_non_co2_ghg.iso_code IS 'ISO code';


--
-- TOC entry 4934 (class 0 OID 0)
-- Dependencies: 226
-- Name: COLUMN agg_non_co2_ghg.population_people; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_non_co2_ghg.population_people IS 'Population';


--
-- TOC entry 4935 (class 0 OID 0)
-- Dependencies: 226
-- Name: COLUMN agg_non_co2_ghg.gdp_usd; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_non_co2_ghg.gdp_usd IS 'Gross domestic product (GDP)';


--
-- TOC entry 4936 (class 0 OID 0)
-- Dependencies: 226
-- Name: COLUMN agg_non_co2_ghg.ghg_excluding_lucf_per_capita_t_per_person; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_non_co2_ghg.ghg_excluding_lucf_per_capita_t_per_person IS 'Per capita greenhouse gas emissions from fossil fuels and industry (measured in tonnes per person of carbon dioxide-equivalents over a 100-year timescale))';


--
-- TOC entry 4937 (class 0 OID 0)
-- Dependencies: 226
-- Name: COLUMN agg_non_co2_ghg.ghg_per_capita_t_per_person; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_non_co2_ghg.ghg_per_capita_t_per_person IS '	Per capita greenhouse gas emissions including land use (measured in tonnes per person of carbon dioxide-equivalents over a 100-year timescale)';


--
-- TOC entry 4938 (class 0 OID 0)
-- Dependencies: 226
-- Name: COLUMN agg_non_co2_ghg.methane_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_non_co2_ghg.methane_mt IS 'Annual methane emissions including land use (measured in tonnes of carbon dioxide-equivalents over a 100-year timescale)';


--
-- TOC entry 4939 (class 0 OID 0)
-- Dependencies: 226
-- Name: COLUMN agg_non_co2_ghg.methane_per_capita_t_per_person; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_non_co2_ghg.methane_per_capita_t_per_person IS 'Per capita methane emissions including land use (measured in tonnes of carbon dioxide-equivalents over a 100-year timescale)';


--
-- TOC entry 4940 (class 0 OID 0)
-- Dependencies: 226
-- Name: COLUMN agg_non_co2_ghg.nitrous_oxide_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_non_co2_ghg.nitrous_oxide_mt IS 'Annual nitrous oxide emissions including land use (measured in tonnes of carbon dioxide-equivalents over a 100-year timescale)';


--
-- TOC entry 4941 (class 0 OID 0)
-- Dependencies: 226
-- Name: COLUMN agg_non_co2_ghg.nitrous_oxide_per_capita_t_per_person; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_non_co2_ghg.nitrous_oxide_per_capita_t_per_person IS 'Per capita nitrous oxide emissions including land use (measured in tonnes of carbon dioxide-equivalents over a 100-year timescale)';


--
-- TOC entry 4942 (class 0 OID 0)
-- Dependencies: 226
-- Name: COLUMN agg_non_co2_ghg.total_ghg_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_non_co2_ghg.total_ghg_mt IS 'Annual greenhouse gas emissions including land use (measured in tonnes of carbon dioxide-equivalents over a 100-year timescale)';


--
-- TOC entry 4943 (class 0 OID 0)
-- Dependencies: 226
-- Name: COLUMN agg_non_co2_ghg.total_ghg_excluding_lucf_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.agg_non_co2_ghg.total_ghg_excluding_lucf_mt IS '	Annual greenhouse gas emissions from fossil fuels and industry (measured in tonnes of carbon dioxide-equivalents over a 100-year timescale)';


--
-- TOC entry 222 (class 1259 OID 246093)
-- Name: fact_climate_impact; Type: TABLE; Schema: co2_project; Owner: -
--

CREATE TABLE co2_project.fact_climate_impact (
    country character varying(255),
    year integer NOT NULL,
    iso_code character varying(30) NOT NULL,
    population_people bigint,
    share_of_temperature_change_from_ghg_prct double precision,
    temperature_change_from_ch4_degrees_c double precision,
    temperature_change_from_co2_degrees_c double precision,
    temperature_change_from_ghg_degrees_c double precision,
    temperature_change_from_n2o_degrees_c double precision
);


--
-- TOC entry 4944 (class 0 OID 0)
-- Dependencies: 222
-- Name: COLUMN fact_climate_impact.country; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_climate_impact.country IS 'Country';


--
-- TOC entry 4945 (class 0 OID 0)
-- Dependencies: 222
-- Name: COLUMN fact_climate_impact.year; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_climate_impact.year IS 'Year';


--
-- TOC entry 4946 (class 0 OID 0)
-- Dependencies: 222
-- Name: COLUMN fact_climate_impact.iso_code; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_climate_impact.iso_code IS 'ISO code';


--
-- TOC entry 4947 (class 0 OID 0)
-- Dependencies: 222
-- Name: COLUMN fact_climate_impact.share_of_temperature_change_from_ghg_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_climate_impact.share_of_temperature_change_from_ghg_prct IS 'Share of contribution to global warming (prct)';


--
-- TOC entry 4948 (class 0 OID 0)
-- Dependencies: 222
-- Name: COLUMN fact_climate_impact.temperature_change_from_ch4_degrees_c; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_climate_impact.temperature_change_from_ch4_degrees_c IS 'Change in global mean surface temperature caused by methane emissions (degrees C)';


--
-- TOC entry 4949 (class 0 OID 0)
-- Dependencies: 222
-- Name: COLUMN fact_climate_impact.temperature_change_from_co2_degrees_c; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_climate_impact.temperature_change_from_co2_degrees_c IS 'Change in global mean surface temperature caused by CO₂ emissions (degrees C)';


--
-- TOC entry 4950 (class 0 OID 0)
-- Dependencies: 222
-- Name: COLUMN fact_climate_impact.temperature_change_from_ghg_degrees_c; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_climate_impact.temperature_change_from_ghg_degrees_c IS 'Change in global mean surface temperature caused by greenhouse gas emissions (degrees C)';


--
-- TOC entry 4951 (class 0 OID 0)
-- Dependencies: 222
-- Name: COLUMN fact_climate_impact.temperature_change_from_n2o_degrees_c; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_climate_impact.temperature_change_from_n2o_degrees_c IS 'Change in global mean surface temperature caused by nitrous oxide emissions (degrees C)';


--
-- TOC entry 219 (class 1259 OID 246078)
-- Name: fact_consumption; Type: TABLE; Schema: co2_project; Owner: -
--

CREATE TABLE co2_project.fact_consumption (
    country character varying(255),
    year integer NOT NULL,
    iso_code character varying(30) NOT NULL,
    population_people bigint,
    gdp_usd double precision,
    consumption_co2_mt double precision,
    consumption_co2_per_capita_t_per_person double precision,
    consumption_co2_per_gdp_kg_per_usd double precision,
    energy_per_capita_kwh double precision,
    energy_per_gdp_kwh double precision,
    primary_energy_consumption_twh double precision
);


--
-- TOC entry 4952 (class 0 OID 0)
-- Dependencies: 219
-- Name: COLUMN fact_consumption.country; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_consumption.country IS 'Country';


--
-- TOC entry 4953 (class 0 OID 0)
-- Dependencies: 219
-- Name: COLUMN fact_consumption.year; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_consumption.year IS 'Year';


--
-- TOC entry 4954 (class 0 OID 0)
-- Dependencies: 219
-- Name: COLUMN fact_consumption.iso_code; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_consumption.iso_code IS 'ISO code';


--
-- TOC entry 4955 (class 0 OID 0)
-- Dependencies: 219
-- Name: COLUMN fact_consumption.population_people; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_consumption.population_people IS 'Population';


--
-- TOC entry 4956 (class 0 OID 0)
-- Dependencies: 219
-- Name: COLUMN fact_consumption.gdp_usd; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_consumption.gdp_usd IS 'Gross domestic product (GDP)';


--
-- TOC entry 4957 (class 0 OID 0)
-- Dependencies: 219
-- Name: COLUMN fact_consumption.consumption_co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_consumption.consumption_co2_mt IS 'Annual consumption-based CO₂ emissions (measured in million tonnes)';


--
-- TOC entry 4958 (class 0 OID 0)
-- Dependencies: 219
-- Name: COLUMN fact_consumption.consumption_co2_per_capita_t_per_person; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_consumption.consumption_co2_per_capita_t_per_person IS 'Per capita consumption-based CO₂ emissions';


--
-- TOC entry 4959 (class 0 OID 0)
-- Dependencies: 219
-- Name: COLUMN fact_consumption.consumption_co2_per_gdp_kg_per_usd; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_consumption.consumption_co2_per_gdp_kg_per_usd IS 'Annual consumption-based CO₂ emissions per GDP (measured in kilograms per dollar of GDP)';


--
-- TOC entry 4960 (class 0 OID 0)
-- Dependencies: 219
-- Name: COLUMN fact_consumption.energy_per_capita_kwh; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_consumption.energy_per_capita_kwh IS 'Primary energy consumption per capita (Measured in kilowatt-hours per person)';


--
-- TOC entry 4961 (class 0 OID 0)
-- Dependencies: 219
-- Name: COLUMN fact_consumption.energy_per_gdp_kwh; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_consumption.energy_per_gdp_kwh IS 'Primary energy consumption per GDP (Measured in kilowatt-hours per USD)';


--
-- TOC entry 4962 (class 0 OID 0)
-- Dependencies: 219
-- Name: COLUMN fact_consumption.primary_energy_consumption_twh; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_consumption.primary_energy_consumption_twh IS '	Primary energy consumption (Measured in terawatt-hours)';


--
-- TOC entry 220 (class 1259 OID 246083)
-- Name: fact_emission_sources; Type: TABLE; Schema: co2_project; Owner: -
--

CREATE TABLE co2_project.fact_emission_sources (
    country character varying(255),
    year integer NOT NULL,
    iso_code character varying(30) NOT NULL,
    population_people bigint,
    gdp_usd double precision,
    cement_co2_mt double precision,
    cement_co2_per_capita_t_per_person double precision,
    coal_co2_mt double precision,
    coal_co2_per_capita_t_per_person double precision,
    cumulative_cement_co2_mt double precision,
    cumulative_coal_co2_mt double precision,
    cumulative_flaring_co2_mt double precision,
    cumulative_gas_co2_mt double precision,
    cumulative_luc_co2_mt double precision,
    cumulative_oil_co2_mt double precision,
    cumulative_other_co2_mt double precision,
    flaring_co2_mt double precision,
    flaring_co2_per_capita_t_per_person double precision,
    gas_co2_mt double precision,
    gas_co2_per_capita_t_per_person double precision,
    land_use_change_co2_mt double precision,
    land_use_change_co2_per_capita_t_per_person double precision,
    oil_co2_mt double precision,
    oil_co2_per_capita_t_per_person double precision,
    other_co2_per_capita_t_per_person double precision,
    other_industry_co2_mt double precision,
    share_global_cement_co2_prct double precision,
    share_global_coal_co2_prct double precision,
    share_global_cumulative_cement_co2_prct double precision,
    share_global_cumulative_coal_co2_prct double precision,
    share_global_cumulative_flaring_co2_prct double precision,
    share_global_cumulative_gas_co2_prct double precision,
    share_global_cumulative_luc_co2_prct double precision,
    share_global_cumulative_oil_co2_prct double precision,
    share_global_cumulative_other_co2_prct double precision,
    share_global_flaring_co2_prct double precision,
    share_global_gas_co2_prct double precision,
    share_global_luc_co2_prct double precision,
    share_global_oil_co2_prct double precision,
    share_global_other_co2_prct double precision,
    trade_co2_mt double precision,
    trade_co2_share_prct double precision
);


--
-- TOC entry 4963 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.country; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.country IS 'Country';


--
-- TOC entry 4964 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.year; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.year IS 'Year';


--
-- TOC entry 4965 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.iso_code; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.iso_code IS 'ISO code';


--
-- TOC entry 4966 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.population_people; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.population_people IS 'Population';


--
-- TOC entry 4967 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.gdp_usd; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.gdp_usd IS 'Gross domestic product (GDP)';


--
-- TOC entry 4968 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.cement_co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.cement_co2_mt IS 'Annual CO₂ emissions from cement (measured in million tonnes)';


--
-- TOC entry 4969 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.cement_co2_per_capita_t_per_person; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.cement_co2_per_capita_t_per_person IS '	Annual CO₂ emissions from cement (per capita)';


--
-- TOC entry 4970 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.coal_co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.coal_co2_mt IS 'Annual CO₂ emissions from coal (measured in million tonnes)';


--
-- TOC entry 4971 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.coal_co2_per_capita_t_per_person; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.coal_co2_per_capita_t_per_person IS 'Annual CO₂ emissions from coal (per capita)';


--
-- TOC entry 4972 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.cumulative_cement_co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.cumulative_cement_co2_mt IS 'Cumulative CO₂ emissions from cement (measured in million tonnes)';


--
-- TOC entry 4973 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.cumulative_coal_co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.cumulative_coal_co2_mt IS 'Cumulative CO₂ emissions from coal (measured in million tonnes)';


--
-- TOC entry 4974 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.cumulative_flaring_co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.cumulative_flaring_co2_mt IS 'Cumulative CO₂ emissions from flaring (measured in million tonnes)';


--
-- TOC entry 4975 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.cumulative_gas_co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.cumulative_gas_co2_mt IS 'Cumulative CO₂ emissions from gas (measured in million tonnes)';


--
-- TOC entry 4976 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.cumulative_luc_co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.cumulative_luc_co2_mt IS 'Cumulative CO₂ emissions from land-use change (measured in million tonnes)';


--
-- TOC entry 4977 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.cumulative_oil_co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.cumulative_oil_co2_mt IS 'Cumulative CO₂ emissions from oil (measured in million tonnes)';


--
-- TOC entry 4978 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.cumulative_other_co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.cumulative_other_co2_mt IS 'Cumulative CO₂ emissions from other industry (measured in million tonnes)';


--
-- TOC entry 4979 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.flaring_co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.flaring_co2_mt IS 'Annual CO₂ emissions from flaring (measured in million tonnes)';


--
-- TOC entry 4980 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.flaring_co2_per_capita_t_per_person; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.flaring_co2_per_capita_t_per_person IS 'Annual CO₂ emissions from flaring (per capita) (measured in tonnes per person)';


--
-- TOC entry 4981 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.gas_co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.gas_co2_mt IS 'Annual CO₂ emissions from gas (measured in million tonnes)';


--
-- TOC entry 4982 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.gas_co2_per_capita_t_per_person; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.gas_co2_per_capita_t_per_person IS 'Annual CO₂ emissions from gas (per capita) (measured in tonnes per person)';


--
-- TOC entry 4983 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.land_use_change_co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.land_use_change_co2_mt IS 'Annual CO₂ emissions from land-use change (measured in million tonnes)';


--
-- TOC entry 4984 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.land_use_change_co2_per_capita_t_per_person; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.land_use_change_co2_per_capita_t_per_person IS 'Annual CO₂ emissions from land-use change per capita (measured in tonnes per person)';


--
-- TOC entry 4985 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.oil_co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.oil_co2_mt IS 'Annual CO₂ emissions from oil (measured in million tonnes)';


--
-- TOC entry 4986 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.oil_co2_per_capita_t_per_person; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.oil_co2_per_capita_t_per_person IS 'Annual CO₂ emissions from oil (per capita) (measured in tonnes per person)';


--
-- TOC entry 4987 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.other_co2_per_capita_t_per_person; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.other_co2_per_capita_t_per_person IS 'Annual CO₂ emissions from other industry (per capita) (measured in tonnes per person)';


--
-- TOC entry 4988 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.other_industry_co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.other_industry_co2_mt IS 'Annual CO₂ emissions from other industry (measured in million tonnes)';


--
-- TOC entry 4989 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.share_global_cement_co2_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.share_global_cement_co2_prct IS 'Share of global annual CO₂ emissions from cement (prct)';


--
-- TOC entry 4990 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.share_global_coal_co2_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.share_global_coal_co2_prct IS 'Share of global annual CO₂ emissions (prct)';


--
-- TOC entry 4991 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.share_global_cumulative_cement_co2_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.share_global_cumulative_cement_co2_prct IS 'Share of global cumulative CO₂ emissions from cement (prct)';


--
-- TOC entry 4992 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.share_global_cumulative_coal_co2_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.share_global_cumulative_coal_co2_prct IS 'Share of global cumulative CO₂ emissions from coal (prct)';


--
-- TOC entry 4993 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.share_global_cumulative_flaring_co2_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.share_global_cumulative_flaring_co2_prct IS 'Share of global cumulative CO₂ emissions from flaring (prct)';


--
-- TOC entry 4994 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.share_global_cumulative_gas_co2_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.share_global_cumulative_gas_co2_prct IS 'Share of global cumulative CO₂ emissions from gas (prct)';


--
-- TOC entry 4995 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.share_global_cumulative_luc_co2_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.share_global_cumulative_luc_co2_prct IS 'Share of global cumulative CO₂ emissions from land-use change (prct)';


--
-- TOC entry 4996 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.share_global_cumulative_oil_co2_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.share_global_cumulative_oil_co2_prct IS 'Share of global cumulative CO₂ emissions from oil (prct)';


--
-- TOC entry 4997 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.share_global_cumulative_other_co2_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.share_global_cumulative_other_co2_prct IS '	Share of global cumulative CO₂ emissions from other industry (prct)';


--
-- TOC entry 4998 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.share_global_flaring_co2_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.share_global_flaring_co2_prct IS 'Share of global annual CO₂ emissions from flaring (prct)';


--
-- TOC entry 4999 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.share_global_gas_co2_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.share_global_gas_co2_prct IS 'Share of global annual CO₂ emissions from gas (prct)';


--
-- TOC entry 5000 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.share_global_luc_co2_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.share_global_luc_co2_prct IS 'Share of global annual CO₂ emissions from land-use change (prct)';


--
-- TOC entry 5001 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.share_global_oil_co2_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.share_global_oil_co2_prct IS 'Share of global annual CO₂ emissions from oil (prct)';


--
-- TOC entry 5002 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.share_global_other_co2_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.share_global_other_co2_prct IS 'Share of global annual CO₂ emissions from other industry (prct)';


--
-- TOC entry 5003 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.trade_co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.trade_co2_mt IS 'Annual CO₂ emissions embedded in trade (measured in million tonnes)';


--
-- TOC entry 5004 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN fact_emission_sources.trade_co2_share_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emission_sources.trade_co2_share_prct IS 'Share of annual CO₂ emissions embedded in trade (prct)';


--
-- TOC entry 218 (class 1259 OID 246070)
-- Name: fact_emissions; Type: TABLE; Schema: co2_project; Owner: -
--

CREATE TABLE co2_project.fact_emissions (
    country character varying(255),
    year integer NOT NULL,
    iso_code character varying(30) NOT NULL,
    population_people bigint,
    gdp_usd double precision,
    co2_mt double precision,
    co2_growth_abs_mt double precision,
    co2_growth_prct_prct double precision,
    co2_including_luc_mt double precision,
    co2_including_luc_growth_abs_mt double precision,
    co2_including_luc_growth_prct_prct double precision,
    co2_including_luc_per_capita_t_per_person double precision,
    co2_including_luc_per_gdp_kg_per_usd double precision,
    co2_including_luc_per_unit_energy_kg_per_kwh double precision,
    cumulative_co2_mt double precision,
    cumulative_co2_including_luc_mt double precision,
    co2_per_capita_t_per_person double precision,
    co2_per_gdp_kg_per_usd double precision,
    co2_per_unit_energy_kg_per_kwh double precision,
    share_global_co2_prct double precision,
    share_global_co2_including_luc_prct double precision,
    share_global_cumulative_co2_prct double precision,
    share_global_cumulative_co2_including_luc_prct double precision
);


--
-- TOC entry 5005 (class 0 OID 0)
-- Dependencies: 218
-- Name: COLUMN fact_emissions.country; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emissions.country IS 'Country';


--
-- TOC entry 5006 (class 0 OID 0)
-- Dependencies: 218
-- Name: COLUMN fact_emissions.year; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emissions.year IS 'Year';


--
-- TOC entry 5007 (class 0 OID 0)
-- Dependencies: 218
-- Name: COLUMN fact_emissions.iso_code; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emissions.iso_code IS 'ISO code';


--
-- TOC entry 5008 (class 0 OID 0)
-- Dependencies: 218
-- Name: COLUMN fact_emissions.population_people; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emissions.population_people IS 'Population';


--
-- TOC entry 5009 (class 0 OID 0)
-- Dependencies: 218
-- Name: COLUMN fact_emissions.gdp_usd; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emissions.gdp_usd IS 'Gross domestic product (GDP)';


--
-- TOC entry 5010 (class 0 OID 0)
-- Dependencies: 218
-- Name: COLUMN fact_emissions.co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emissions.co2_mt IS 'Annual CO₂ emissions (measured in million tonnes)';


--
-- TOC entry 5011 (class 0 OID 0)
-- Dependencies: 218
-- Name: COLUMN fact_emissions.co2_growth_abs_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emissions.co2_growth_abs_mt IS 'Annual CO₂ emissions growth (abs) (measured in million tonnes)';


--
-- TOC entry 5012 (class 0 OID 0)
-- Dependencies: 218
-- Name: COLUMN fact_emissions.co2_growth_prct_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emissions.co2_growth_prct_prct IS 'Annual CO₂ emissions growth (prct)';


--
-- TOC entry 5013 (class 0 OID 0)
-- Dependencies: 218
-- Name: COLUMN fact_emissions.co2_including_luc_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emissions.co2_including_luc_mt IS 'Annual CO₂ emissions including land-use change (measured in million tonnes)';


--
-- TOC entry 5014 (class 0 OID 0)
-- Dependencies: 218
-- Name: COLUMN fact_emissions.co2_including_luc_growth_abs_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emissions.co2_including_luc_growth_abs_mt IS 'Growth rate of emissions including land-use change (measured in million tonnes)';


--
-- TOC entry 5015 (class 0 OID 0)
-- Dependencies: 218
-- Name: COLUMN fact_emissions.co2_including_luc_growth_prct_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emissions.co2_including_luc_growth_prct_prct IS 'Growth rate of emissions including land-use change (prct)';


--
-- TOC entry 5016 (class 0 OID 0)
-- Dependencies: 218
-- Name: COLUMN fact_emissions.co2_including_luc_per_capita_t_per_person; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emissions.co2_including_luc_per_capita_t_per_person IS 'Annual CO₂ emissions including land-use change per capita';


--
-- TOC entry 5017 (class 0 OID 0)
-- Dependencies: 218
-- Name: COLUMN fact_emissions.co2_including_luc_per_gdp_kg_per_usd; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emissions.co2_including_luc_per_gdp_kg_per_usd IS 'Annual CO₂ emissions including land-use change per GDP';


--
-- TOC entry 5018 (class 0 OID 0)
-- Dependencies: 218
-- Name: COLUMN fact_emissions.co2_including_luc_per_unit_energy_kg_per_kwh; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emissions.co2_including_luc_per_unit_energy_kg_per_kwh IS 'Annual CO₂ emissions including land-use change per unit energy';


--
-- TOC entry 5019 (class 0 OID 0)
-- Dependencies: 218
-- Name: COLUMN fact_emissions.cumulative_co2_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emissions.cumulative_co2_mt IS 'Cumulative CO₂ emissions (measured in million tonnes)';


--
-- TOC entry 5020 (class 0 OID 0)
-- Dependencies: 218
-- Name: COLUMN fact_emissions.cumulative_co2_including_luc_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emissions.cumulative_co2_including_luc_mt IS 'Cumulative CO₂ emissions including land-use change (measured in million tonnes)';


--
-- TOC entry 5021 (class 0 OID 0)
-- Dependencies: 218
-- Name: COLUMN fact_emissions.co2_per_capita_t_per_person; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emissions.co2_per_capita_t_per_person IS 'CO₂ emissions per capita';


--
-- TOC entry 5022 (class 0 OID 0)
-- Dependencies: 218
-- Name: COLUMN fact_emissions.co2_per_gdp_kg_per_usd; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emissions.co2_per_gdp_kg_per_usd IS 'Annual CO₂ emissions per GDP ';


--
-- TOC entry 5023 (class 0 OID 0)
-- Dependencies: 218
-- Name: COLUMN fact_emissions.co2_per_unit_energy_kg_per_kwh; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emissions.co2_per_unit_energy_kg_per_kwh IS 'Annual CO₂ emissions per unit energy';


--
-- TOC entry 5024 (class 0 OID 0)
-- Dependencies: 218
-- Name: COLUMN fact_emissions.share_global_co2_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emissions.share_global_co2_prct IS 'Share of global annual CO₂ emissions (prct)';


--
-- TOC entry 5025 (class 0 OID 0)
-- Dependencies: 218
-- Name: COLUMN fact_emissions.share_global_co2_including_luc_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emissions.share_global_co2_including_luc_prct IS 'Share of global annual CO₂ emissions including land-use change (prct)';


--
-- TOC entry 5026 (class 0 OID 0)
-- Dependencies: 218
-- Name: COLUMN fact_emissions.share_global_cumulative_co2_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emissions.share_global_cumulative_co2_prct IS 'Share of global cumulative CO₂ emissions (prct)';


--
-- TOC entry 5027 (class 0 OID 0)
-- Dependencies: 218
-- Name: COLUMN fact_emissions.share_global_cumulative_co2_including_luc_prct; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_emissions.share_global_cumulative_co2_including_luc_prct IS 'Share of global cumulative CO₂ emissions including land-use change (prct)';


--
-- TOC entry 221 (class 1259 OID 246088)
-- Name: fact_non_co2_ghg; Type: TABLE; Schema: co2_project; Owner: -
--

CREATE TABLE co2_project.fact_non_co2_ghg (
    country character varying(255),
    year integer NOT NULL,
    iso_code character varying(30) NOT NULL,
    population_people bigint,
    gdp_usd double precision,
    ghg_excluding_lucf_per_capita_t_per_person double precision,
    ghg_per_capita_t_per_person double precision,
    methane_mt double precision,
    methane_per_capita_t_per_person double precision,
    nitrous_oxide_mt double precision,
    nitrous_oxide_per_capita_t_per_person double precision,
    total_ghg_mt double precision,
    total_ghg_excluding_lucf_mt double precision
);


--
-- TOC entry 5028 (class 0 OID 0)
-- Dependencies: 221
-- Name: COLUMN fact_non_co2_ghg.country; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_non_co2_ghg.country IS 'Country';


--
-- TOC entry 5029 (class 0 OID 0)
-- Dependencies: 221
-- Name: COLUMN fact_non_co2_ghg.year; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_non_co2_ghg.year IS 'Year';


--
-- TOC entry 5030 (class 0 OID 0)
-- Dependencies: 221
-- Name: COLUMN fact_non_co2_ghg.iso_code; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_non_co2_ghg.iso_code IS 'ISO code';


--
-- TOC entry 5031 (class 0 OID 0)
-- Dependencies: 221
-- Name: COLUMN fact_non_co2_ghg.population_people; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_non_co2_ghg.population_people IS 'Population';


--
-- TOC entry 5032 (class 0 OID 0)
-- Dependencies: 221
-- Name: COLUMN fact_non_co2_ghg.gdp_usd; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_non_co2_ghg.gdp_usd IS 'Gross domestic product (GDP)';


--
-- TOC entry 5033 (class 0 OID 0)
-- Dependencies: 221
-- Name: COLUMN fact_non_co2_ghg.ghg_excluding_lucf_per_capita_t_per_person; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_non_co2_ghg.ghg_excluding_lucf_per_capita_t_per_person IS 'Per capita greenhouse gas emissions from fossil fuels and industry (measured in tonnes per person of carbon dioxide-equivalents over a 100-year timescale))';


--
-- TOC entry 5034 (class 0 OID 0)
-- Dependencies: 221
-- Name: COLUMN fact_non_co2_ghg.ghg_per_capita_t_per_person; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_non_co2_ghg.ghg_per_capita_t_per_person IS '	Per capita greenhouse gas emissions including land use (measured in tonnes per person of carbon dioxide-equivalents over a 100-year timescale)';


--
-- TOC entry 5035 (class 0 OID 0)
-- Dependencies: 221
-- Name: COLUMN fact_non_co2_ghg.methane_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_non_co2_ghg.methane_mt IS 'Annual methane emissions including land use (measured in tonnes of carbon dioxide-equivalents over a 100-year timescale)';


--
-- TOC entry 5036 (class 0 OID 0)
-- Dependencies: 221
-- Name: COLUMN fact_non_co2_ghg.methane_per_capita_t_per_person; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_non_co2_ghg.methane_per_capita_t_per_person IS 'Per capita methane emissions including land use (measured in tonnes of carbon dioxide-equivalents over a 100-year timescale)';


--
-- TOC entry 5037 (class 0 OID 0)
-- Dependencies: 221
-- Name: COLUMN fact_non_co2_ghg.nitrous_oxide_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_non_co2_ghg.nitrous_oxide_mt IS 'Annual nitrous oxide emissions including land use (measured in tonnes of carbon dioxide-equivalents over a 100-year timescale)';


--
-- TOC entry 5038 (class 0 OID 0)
-- Dependencies: 221
-- Name: COLUMN fact_non_co2_ghg.nitrous_oxide_per_capita_t_per_person; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_non_co2_ghg.nitrous_oxide_per_capita_t_per_person IS 'Per capita nitrous oxide emissions including land use (measured in tonnes of carbon dioxide-equivalents over a 100-year timescale)';


--
-- TOC entry 5039 (class 0 OID 0)
-- Dependencies: 221
-- Name: COLUMN fact_non_co2_ghg.total_ghg_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_non_co2_ghg.total_ghg_mt IS 'Annual greenhouse gas emissions including land use (measured in tonnes of carbon dioxide-equivalents over a 100-year timescale)';


--
-- TOC entry 5040 (class 0 OID 0)
-- Dependencies: 221
-- Name: COLUMN fact_non_co2_ghg.total_ghg_excluding_lucf_mt; Type: COMMENT; Schema: co2_project; Owner: -
--

COMMENT ON COLUMN co2_project.fact_non_co2_ghg.total_ghg_excluding_lucf_mt IS '	Annual greenhouse gas emissions from fossil fuels and industry (measured in tonnes of carbon dioxide-equivalents over a 100-year timescale)';


--
-- TOC entry 4692 (class 2606 OID 246127)
-- Name: agg_climate_impact agg_climate_impact_pkey; Type: CONSTRAINT; Schema: co2_project; Owner: -
--

ALTER TABLE ONLY co2_project.agg_climate_impact
    ADD CONSTRAINT agg_climate_impact_pkey PRIMARY KEY (year, country);


--
-- TOC entry 4690 (class 2606 OID 246122)
-- Name: agg_consumption agg_consumption_pkey; Type: CONSTRAINT; Schema: co2_project; Owner: -
--

ALTER TABLE ONLY co2_project.agg_consumption
    ADD CONSTRAINT agg_consumption_pkey PRIMARY KEY (year, country);


--
-- TOC entry 4696 (class 2606 OID 246137)
-- Name: agg_emission_sources agg_emission_sources_pkey; Type: CONSTRAINT; Schema: co2_project; Owner: -
--

ALTER TABLE ONLY co2_project.agg_emission_sources
    ADD CONSTRAINT agg_emission_sources_pkey PRIMARY KEY (year, country);


--
-- TOC entry 4688 (class 2606 OID 246117)
-- Name: agg_emissions agg_emissions_pkey; Type: CONSTRAINT; Schema: co2_project; Owner: -
--

ALTER TABLE ONLY co2_project.agg_emissions
    ADD CONSTRAINT agg_emissions_pkey PRIMARY KEY (year, country);


--
-- TOC entry 4694 (class 2606 OID 246132)
-- Name: agg_non_co2_ghg agg_non_co2_ghg_pkey; Type: CONSTRAINT; Schema: co2_project; Owner: -
--

ALTER TABLE ONLY co2_project.agg_non_co2_ghg
    ADD CONSTRAINT agg_non_co2_ghg_pkey PRIMARY KEY (year, country);


--
-- TOC entry 4686 (class 2606 OID 246097)
-- Name: fact_climate_impact fact_climate_impact_pkey; Type: CONSTRAINT; Schema: co2_project; Owner: -
--

ALTER TABLE ONLY co2_project.fact_climate_impact
    ADD CONSTRAINT fact_climate_impact_pkey PRIMARY KEY (year, iso_code);


--
-- TOC entry 4680 (class 2606 OID 246082)
-- Name: fact_consumption fact_consumption_pkey; Type: CONSTRAINT; Schema: co2_project; Owner: -
--

ALTER TABLE ONLY co2_project.fact_consumption
    ADD CONSTRAINT fact_consumption_pkey PRIMARY KEY (year, iso_code);


--
-- TOC entry 4682 (class 2606 OID 246087)
-- Name: fact_emission_sources fact_emission_sources_pkey; Type: CONSTRAINT; Schema: co2_project; Owner: -
--

ALTER TABLE ONLY co2_project.fact_emission_sources
    ADD CONSTRAINT fact_emission_sources_pkey PRIMARY KEY (year, iso_code);


--
-- TOC entry 4678 (class 2606 OID 246074)
-- Name: fact_emissions fact_emissions_pkey; Type: CONSTRAINT; Schema: co2_project; Owner: -
--

ALTER TABLE ONLY co2_project.fact_emissions
    ADD CONSTRAINT fact_emissions_pkey PRIMARY KEY (year, iso_code);


--
-- TOC entry 4684 (class 2606 OID 246092)
-- Name: fact_non_co2_ghg fact_non_co2_ghg_pkey; Type: CONSTRAINT; Schema: co2_project; Owner: -
--

ALTER TABLE ONLY co2_project.fact_non_co2_ghg
    ADD CONSTRAINT fact_non_co2_ghg_pkey PRIMARY KEY (year, iso_code);


-- Completed on 2026-03-26 11:03:53

--
-- PostgreSQL database dump complete
--

