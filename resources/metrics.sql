-- Complete database creation script for Crossbow Metrics optimized for CockroachDB
-- First, create the database if it doesn't exist already
-- Run this part as a superuser/admin

-- CREATE DATABASE IF NOT EXISTS metrics;
-- USE metrics;

-- Create schemas (in CockroachDB public schema exists by default)
-- CREATE SCHEMA IF NOT EXISTS public;

-- Create tables with optimized schema

-- Hosts table
CREATE TABLE public.hosts (
    host_id UUID NOT NULL DEFAULT gen_random_uuid(),
    hostname VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp(),
    CONSTRAINT hosts_pkey PRIMARY KEY (host_id),
    CONSTRAINT hosts_hostname_key UNIQUE (hostname)
);

-- Zones table
CREATE TABLE public.zones (
    zone_id UUID NOT NULL DEFAULT gen_random_uuid(),
    host_id UUID NOT NULL,
    zone_name VARCHAR(64) NOT NULL,
    zone_status VARCHAR(20) NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp(),
    CONSTRAINT zones_pkey PRIMARY KEY (zone_id),
    CONSTRAINT zones_host_id_fkey FOREIGN KEY (host_id) REFERENCES public.hosts(host_id) ON DELETE CASCADE,
    CONSTRAINT zones_host_zone_unique UNIQUE (host_id, zone_name)
);

-- Interfaces table
CREATE TABLE public.interfaces (
    interface_id UUID NOT NULL DEFAULT gen_random_uuid(),
    host_id UUID NOT NULL,
    zone_id UUID NULL,
    interface_name VARCHAR(64) NOT NULL,
    interface_type VARCHAR(20) NOT NULL DEFAULT 'unknown',
    parent_interface VARCHAR(64) NULL,
    mac_address VARCHAR(17) NULL,
    mtu INT8 NULL,
    is_active BOOL NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp(),
    CONSTRAINT interfaces_pkey PRIMARY KEY (interface_id),
    CONSTRAINT interfaces_host_id_fkey FOREIGN KEY (host_id) REFERENCES public.hosts(host_id) ON DELETE CASCADE,
    CONSTRAINT interfaces_zone_id_fkey FOREIGN KEY (zone_id) REFERENCES public.zones(zone_id),
    CONSTRAINT interfaces_unique UNIQUE (host_id, zone_id, interface_name)
);

-- Network metrics table
CREATE TABLE public.netmetrics (
    metric_id UUID NOT NULL DEFAULT gen_random_uuid(),
    interface_id UUID NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT current_timestamp(),
    input_bytes INT8 NOT NULL DEFAULT 0,
    input_packets INT8 NOT NULL DEFAULT 0,
    output_bytes INT8 NOT NULL DEFAULT 0,
    output_packets INT8 NOT NULL DEFAULT 0,
    collection_method VARCHAR(64) NOT NULL DEFAULT 'dlstat',
    CONSTRAINT netmetrics_pkey PRIMARY KEY (metric_id),
    CONSTRAINT netmetrics_interface_id_fkey FOREIGN KEY (interface_id) REFERENCES public.interfaces(interface_id) ON DELETE CASCADE
);

-- MAC Address History Table Schema
CREATE TABLE public.mac_address_history (
    history_id UUID NOT NULL DEFAULT gen_random_uuid(),
    interface_id UUID NOT NULL,
    mac_address VARCHAR(17) NOT NULL,
    effective_from TIMESTAMPTZ NOT NULL DEFAULT current_timestamp(),
    effective_to TIMESTAMPTZ NULL,
    change_reason VARCHAR(64) NULL,
    CONSTRAINT mac_history_pkey PRIMARY KEY (history_id),
    CONSTRAINT mac_history_interface_fkey FOREIGN KEY (interface_id) REFERENCES public.interfaces(interface_id) ON DELETE CASCADE
);

-- Create optimized indices

-- Index on zones.host_id for better join performance
CREATE INDEX idx_zones_host_id ON public.zones(host_id);

-- Indices for interfaces
CREATE INDEX idx_interfaces_zone_id ON public.interfaces(zone_id);
CREATE INDEX idx_interfaces_host_id ON public.interfaces(host_id);
CREATE INDEX idx_interfaces_mac ON public.interfaces(mac_address) WHERE mac_address IS NOT NULL;

-- Indices for metrics
CREATE INDEX idx_netmetrics_timestamp ON public.netmetrics(timestamp);
CREATE INDEX idx_netmetrics_interface_timestamp ON public.netmetrics(interface_id, timestamp);

-- -- Indices for address history
CREATE INDEX idx_mac_history_interface ON public.mac_address_history(interface_id);
CREATE INDEX idx_mac_history_mac ON public.mac_address_history(mac_address);
CREATE INDEX idx_mac_history_timespan ON public.mac_address_history(effective_from, effective_to);

-- Create views

-- Main view for all metrics
CREATE VIEW public.interface_metrics_view AS
SELECT
    m.metric_id,
    m.timestamp,
    m.input_bytes,
    m.input_packets,
    m.output_bytes,
    m.output_packets,
    h.hostname,
    i.interface_name,
    i.interface_type,
    i.mac_address,
    i.mtu,
    z.zone_name
FROM
    public.netmetrics AS m
    JOIN public.interfaces AS i ON m.interface_id = i.interface_id
    JOIN public.hosts AS h ON i.host_id = h.host_id
    LEFT JOIN public.zones AS z ON i.zone_id = z.zone_id;

-- View for non-zero metrics
CREATE VIEW public.active_interface_metrics_view AS
SELECT
    m.metric_id,
    m.timestamp,
    m.input_bytes,
    m.input_packets,
    m.output_bytes,
    m.output_packets,
    h.hostname,
    i.interface_name,
    i.interface_type,
    i.mac_address,
    i.mtu,
    z.zone_name
FROM
    public.netmetrics AS m
    JOIN public.interfaces AS i ON m.interface_id = i.interface_id
    JOIN public.hosts AS h ON i.host_id = h.host_id
    LEFT JOIN public.zones AS z ON i.zone_id = z.zone_id
WHERE
    (m.input_bytes != 0 OR m.output_bytes != 0 OR
     m.input_packets != 0 OR m.output_packets != 0);

-- View to easily see current and historical MAC addresses
CREATE VIEW public.mac_address_view AS
SELECT
    h.hostname,
    z.zone_name,
    i.interface_name,
    i.interface_type,
    i.mac_address AS current_mac,
    mh.mac_address AS historical_mac,
    mh.effective_from,
    mh.effective_to,
    CASE WHEN mh.effective_to IS NULL THEN 'Current' ELSE 'Historical' END AS status
FROM
    public.interfaces i
    JOIN public.hosts h ON i.host_id = h.host_id
    LEFT JOIN public.zones z ON i.zone_id = z.zone_id
    LEFT JOIN public.mac_address_history mh ON i.interface_id = mh.interface_id
ORDER BY
    h.hostname, i.interface_name, mh.effective_from DESC;
