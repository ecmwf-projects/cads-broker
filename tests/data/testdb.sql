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

--
-- Name: status; Type: TYPE; Schema: public; Owner: broker
--

CREATE TYPE public.status AS ENUM (
    'queued',
    'running',
    'failed',
    'completed'
);


ALTER TYPE public.status OWNER TO broker;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: system_requests; Type: TABLE; Schema: public; Owner: broker
--

CREATE TABLE public.system_requests (
    request_id integer NOT NULL,
    request_uid character varying(1024),
    status public.status,
    request_body jsonb NOT NULL,
    request_metadata jsonb,
    response_body jsonb,
    response_metadata jsonb,
    expire timestamp without time zone
);


ALTER TABLE public.system_requests OWNER TO broker;

--
-- Name: system_requests_request_id_seq; Type: SEQUENCE; Schema: public; Owner: broker
--

CREATE SEQUENCE public.system_requests_request_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.system_requests_request_id_seq OWNER TO broker;

--
-- Name: system_requests_request_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: broker
--

ALTER SEQUENCE public.system_requests_request_id_seq OWNED BY public.system_requests.request_id;


--
-- Name: system_requests request_id; Type: DEFAULT; Schema: public; Owner: broker
--

ALTER TABLE ONLY public.system_requests ALTER COLUMN request_id SET DEFAULT nextval('public.system_requests_request_id_seq'::regclass);


--
-- Data for Name: system_requests; Type: TABLE DATA; Schema: public; Owner: broker
--

COPY public.system_requests (request_id, request_uid, status, request_body, request_metadata, response_body, response_metadata, expire) FROM stdin;
\.


--
-- Name: system_requests_request_id_seq; Type: SEQUENCE SET; Schema: public; Owner: broker
--

SELECT pg_catalog.setval('public.system_requests_request_id_seq', 1, false);


--
-- Name: system_requests system_requests_pkey; Type: CONSTRAINT; Schema: public; Owner: broker
--

ALTER TABLE ONLY public.system_requests
    ADD CONSTRAINT system_requests_pkey PRIMARY KEY (request_id);


--
-- Name: ix_system_requests_request_uid; Type: INDEX; Schema: public; Owner: broker
--

CREATE INDEX ix_system_requests_request_uid ON public.system_requests USING btree (request_uid);


--
-- PostgreSQL database dump complete
--
