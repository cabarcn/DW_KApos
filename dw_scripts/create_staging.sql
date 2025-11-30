-- ======================================
-- TABLAS DE STAGING PARA KApos
-- Esquema: stg
-- Datos crudos / sintéticos
-- ======================================

-- Usuarios / Socios
CREATE TABLE IF NOT EXISTS stg_socio (
    socio_id INT,
    rut VARCHAR(20),
    genero VARCHAR(10),
    edad INT,
    comuna VARCHAR(50),
    fecha_ingreso DATE,
    estado VARCHAR(30)
);

-- Suscripciones
CREATE TABLE IF NOT EXISTS stg_subscription (
    subscription_id INT,
    socio_id INT,
    plan_id INT,
    organizacion_id INT,
    metodopago_id INT,
    psp_id INT,
    captador_id INT,
    fecha_inicio DATE,
    fecha_fin DATE,
    estado VARCHAR(20)
);

-- Pagos (Payments)
CREATE TABLE IF NOT EXISTS stg_payment (
    payment_id INT,
    subscription_id INT,
    fecha_pago DATE,
    monto_intentado NUMERIC(14,2),
    monto_pagado NUMERIC(14,2),
    estado_pago VARCHAR(20), -- OK, FALLIDO
    metodo_pago VARCHAR(50)
);

-- Conciliaciones / Settlements
CREATE TABLE IF NOT EXISTS stg_settlement (
    settlement_id INT,
    payment_id INT,
    fecha_abono DATE,
    monto_abonado NUMERIC(14,2),
    dias_hasta_abono INT
);

-- Planes
CREATE TABLE IF NOT EXISTS stg_plan (
    plan_id INT,
    nombre_plan VARCHAR(100),
    monto_mensual NUMERIC(14,2),
    periodicidad VARCHAR(20),
    tipo_plan VARCHAR(50)
);

-- Organizaciones
CREATE TABLE IF NOT EXISTS stg_organizacion (
    organizacion_id INT,
    nombre_org VARCHAR(100),
    rut VARCHAR(20),
    region VARCHAR(50),
    segmento VARCHAR(50),
    fecha_ingreso DATE
);

-- Métodos de pago
CREATE TABLE IF NOT EXISTS stg_metodo_pago (
    metodopago_id INT,
    tipo_metodo VARCHAR(50),
    entidad_bancaria VARCHAR(50),
    categoria VARCHAR(50)
);

-- PSP
CREATE TABLE IF NOT EXISTS stg_psp (
    psp_id INT,
    nombre_psp VARCHAR(50),
    pais VARCHAR(50),
    tipo_conciliacion VARCHAR(50)
);

-- Captadores
CREATE TABLE IF NOT EXISTS stg_captador (
    captador_id INT,
    nombre_captador VARCHAR(100),
    canal VARCHAR(50),
    tipo_captacion VARCHAR(50),
    campania VARCHAR(100),
    fecha_inicio_campania DATE
);
