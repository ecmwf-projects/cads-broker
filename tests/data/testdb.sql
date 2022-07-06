--
-- PostgreSQL database dump
--

-- Dumped from database version 14.4
-- Dumped by pg_dump version 14.4

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: licences; Type: TABLE; Schema: public; Owner: broker
--

CREATE TABLE public.licences (
    licence_id integer NOT NULL,
    licence_uid character varying NOT NULL,
    revision integer NOT NULL,
    title character varying NOT NULL,
    download_filename character varying NOT NULL
);


ALTER TABLE public.licences OWNER TO broker;

--
-- Name: licences_licence_id_seq; Type: SEQUENCE; Schema: public; Owner: broker
--

CREATE SEQUENCE public.licences_licence_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.licences_licence_id_seq OWNER TO broker;

--
-- Name: licences_licence_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: broker
--

ALTER SEQUENCE public.licences_licence_id_seq OWNED BY public.licences.licence_id;


--
-- Name: resources; Type: TABLE; Schema: public; Owner: broker
--

CREATE TABLE public.resources (
    resource_id integer NOT NULL,
    resource_uid character varying NOT NULL,
    title character varying,
    description jsonb NOT NULL,
    abstract text NOT NULL,
    contact character varying(300)[],
    form character varying,
    constraints character varying,
    keywords character varying(300)[],
    version character varying(300),
    variables jsonb,
    providers jsonb,
    summaries jsonb,
    extent jsonb,
    documentation jsonb,
    type character varying(300) NOT NULL,
    previewimage character varying,
    publication_date date,
    record_update timestamp with time zone,
    "references" jsonb[],
    resource_update date,
    use_eqc boolean
);


ALTER TABLE public.resources OWNER TO broker;

--
-- Name: resources_licences; Type: TABLE; Schema: public; Owner: broker
--

CREATE TABLE public.resources_licences (
    resource_id integer NOT NULL,
    licence_id integer NOT NULL
);


ALTER TABLE public.resources_licences OWNER TO broker;

--
-- Name: resources_resource_id_seq; Type: SEQUENCE; Schema: public; Owner: broker
--

CREATE SEQUENCE public.resources_resource_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.resources_resource_id_seq OWNER TO broker;

--
-- Name: resources_resource_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: broker
--

ALTER SEQUENCE public.resources_resource_id_seq OWNED BY public.resources.resource_id;


--
-- Name: licences licence_id; Type: DEFAULT; Schema: public; Owner: broker
--

ALTER TABLE ONLY public.licences ALTER COLUMN licence_id SET DEFAULT nextval('public.licences_licence_id_seq'::regclass);


--
-- Name: resources resource_id; Type: DEFAULT; Schema: public; Owner: broker
--

ALTER TABLE ONLY public.resources ALTER COLUMN resource_id SET DEFAULT nextval('public.resources_resource_id_seq'::regclass);


--
-- Data for Name: licences; Type: TABLE DATA; Schema: public; Owner: broker
--

COPY public.licences (licence_id, licence_uid, revision, title, download_filename) FROM stdin;
\.


--
-- Data for Name: resources; Type: TABLE DATA; Schema: public; Owner: broker
--

COPY public.resources (resource_id, resource_uid, title, description, abstract, contact, form, constraints, keywords, version, variables, providers, summaries, extent, documentation, type, previewimage, publication_date, record_update, "references", resource_update, use_eqc) FROM stdin;
\.


--
-- Data for Name: resources_licences; Type: TABLE DATA; Schema: public; Owner: broker
--

COPY public.resources_licences (resource_id, licence_id) FROM stdin;
\.


--
-- Name: licences_licence_id_seq; Type: SEQUENCE SET; Schema: public; Owner: broker
--

SELECT pg_catalog.setval('public.licences_licence_id_seq', 1, false);


--
-- Name: resources_resource_id_seq; Type: SEQUENCE SET; Schema: public; Owner: broker
--

SELECT pg_catalog.setval('public.resources_resource_id_seq', 1, false);


--
-- Name: licences licence_uid_revision_uc; Type: CONSTRAINT; Schema: public; Owner: broker
--

ALTER TABLE ONLY public.licences
    ADD CONSTRAINT licence_uid_revision_uc UNIQUE (licence_uid, revision);


--
-- Name: licences licences_pkey; Type: CONSTRAINT; Schema: public; Owner: broker
--

ALTER TABLE ONLY public.licences
    ADD CONSTRAINT licences_pkey PRIMARY KEY (licence_id);


--
-- Name: resources_licences resources_licences_pkey; Type: CONSTRAINT; Schema: public; Owner: broker
--

ALTER TABLE ONLY public.resources_licences
    ADD CONSTRAINT resources_licences_pkey PRIMARY KEY (resource_id, licence_id);


--
-- Name: resources resources_pkey; Type: CONSTRAINT; Schema: public; Owner: broker
--

ALTER TABLE ONLY public.resources
    ADD CONSTRAINT resources_pkey PRIMARY KEY (resource_id);


--
-- Name: ix_licences_licence_uid; Type: INDEX; Schema: public; Owner: broker
--

CREATE INDEX ix_licences_licence_uid ON public.licences USING btree (licence_uid);


--
-- Name: ix_licences_revision; Type: INDEX; Schema: public; Owner: broker
--

CREATE INDEX ix_licences_revision ON public.licences USING btree (revision);


--
-- Name: ix_resources_resource_uid; Type: INDEX; Schema: public; Owner: broker
--

CREATE UNIQUE INDEX ix_resources_resource_uid ON public.resources USING btree (resource_uid);


--
-- Name: resources_licences resources_licences_licence_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: broker
--

ALTER TABLE ONLY public.resources_licences
    ADD CONSTRAINT resources_licences_licence_id_fkey FOREIGN KEY (licence_id) REFERENCES public.licences(licence_id);


--
-- Name: resources_licences resources_licences_resource_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: broker
--

ALTER TABLE ONLY public.resources_licences
    ADD CONSTRAINT resources_licences_resource_id_fkey FOREIGN KEY (resource_id) REFERENCES public.resources(resource_id);


--
-- PostgreSQL database dump complete
--

