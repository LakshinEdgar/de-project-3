# Проект 2
Опишите здесь поэтапно ход решения задачи. Вы можете ориентироваться на тот план выполнения проекта, который мы предлагаем в инструкции на платформе.


```SQL
-- Создайте справочник стоимости доставки в страны 
DROP TABLE IF EXISTS public.shipping_country_rates CASCADE;
CREATE TABLE public.shipping_country_rates (id SERIAL, 
                                            shipping_country TEXT,
                                            shipping_country_base_rate NUMERIC(14,3),
                                            PRIMARY KEY (id)
                                            );
INSERT INTO public.shipping_country_rates (shipping_country,
                                           shipping_country_base_rate
                                           )
SELECT DISTINCT shipping_country, 
                shipping_country_base_rate
FROM public.shipping;

-- Создайте справочник тарифов доставки вендора по договору
DROP TABLE IF EXISTS public.shipping_agreement CASCADE;
CREATE TABLE public.shipping_agreement (agreementid BIGINT, 
                                       agreement_number VARCHAR(20),
                                       agreement_rate NUMERIC(14,3),
                                       agreement_commission NUMERIC(14,3),
                                       PRIMARY KEY (agreementid)
                                        );
INSERT INTO public.shipping_agreement (agreementid,
                                       agreement_number,
                                       agreement_rate,
                                       agreement_commission
                                       )
SELECT DISTINCT (regexp_split_to_array(vendor_agreement_description, E'\\:+'))[1]::INT AS agreementid,
                (regexp_split_to_array(vendor_agreement_description, E'\\:+'))[2]::VARCHAR(20) AS agreement_number,
                (regexp_split_to_array(vendor_agreement_description, E'\\:+'))[3]::NUMERIC(14,3) AS agreement_rate,
                (regexp_split_to_array(vendor_agreement_description, E'\\:+'))[4]::NUMERIC(14,3) AS agreement_commission
FROM public.shipping;

-- Создайте справочник о типах доставки
DROP TABLE IF EXISTS public.shipping_transfer CASCADE;
CREATE TABLE public.shipping_transfer (id SERIAL, 
                                      transfer_type VARCHAR(20),
                                      transfer_model VARCHAR(20),
                                      shipping_transfer_rate NUMERIC(14,3),
                                      PRIMARY KEY (id)
                                      );
INSERT INTO public.shipping_transfer (transfer_type,
                                      transfer_model,
                                      shipping_transfer_rate
                                      )                                      
SELECT DISTINCT (regexp_split_to_array(shipping_transfer_description, E'\\:+'))[1]::VARCHAR(20) AS transfer_type,
                (regexp_split_to_array(shipping_transfer_description, E'\\:+'))[2]::VARCHAR(20) AS transfer_model,
                shipping_transfer_rate::NUMERIC(14,3)
FROM public.shipping;

-- Создайте таблицу shipping_info с уникальными доставками shippingid
DROP TABLE IF EXISTS public.shipping_info CASCADE;
CREATE TABLE public.shipping_info (shippingid BIGINT,
                                   vendorid BIGINT,
                                   payment_amount NUMERIC(14,2),
                                   shipping_plan_datetime TIMESTAMP,
                                   transfer_type_id BIGINT,
                                   agreementid BIGINT,
                                   shipping_country_id BIGINT,
                                   PRIMARY KEY (shippingid),
                                   FOREIGN KEY (transfer_type_id) REFERENCES public.shipping_transfer(id) ON UPDATE CASCADE,
                                   FOREIGN KEY (agreementid) REFERENCES public.shipping_agreement(agreementid) ON UPDATE CASCADE,
                                   FOREIGN KEY (shipping_country_id) REFERENCES public.shipping_country_rates(id) ON UPDATE CASCADE
                               );
INSERT INTO public.shipping_info (shippingid,
       vendorid,
       payment_amount,
       shipping_plan_datetime,
       transfer_type_id,
       agreementid,
       shipping_country_id)
       
SELECT DISTINCT
       shippingid::BIGINT,
       vendorid::BIGINT,
       payment_amount::NUMERIC(14,2),
       shipping_plan_datetime::TIMESTAMP,
       st.id::BIGINT AS transfer_type_id,
       (regexp_split_to_array(vendor_agreement_description, E'\\:+'))[1]::BIGINT AS agreementid,
       scr.id::BIGINT AS shipping_country_id
FROM public.shipping AS s
JOIN public.shipping_transfer AS st 
     ON st.transfer_type=(regexp_split_to_array(s.shipping_transfer_description, E'\\:+'))[1]::VARCHAR(20) 
     AND st.transfer_model=(regexp_split_to_array(s.shipping_transfer_description, E'\\:+'))[2]::VARCHAR(20)
     AND st.shipping_transfer_rate = s.shipping_transfer_rate
JOIN public.shipping_country_rates AS scr 
     ON scr.shipping_country=s.shipping_country 
     AND scr.shipping_country_base_rate=s.shipping_country_base_rate;

 -- Создайте таблицу статусов о доставке shipping_status
DROP TABLE IF EXISTS public.shipping_status CASCADE;
CREATE TABLE public.shipping_status (shippingid BIGINT,
                                     status VARCHAR(20),
                                     state VARCHAR(20),
                                     shipping_start_fact_datetime TIMESTAMP,
                                     shipping_end_fact_datetime TIMESTAMP
                                     ); 
 
 WITH s AS (SELECT orderid,
                  shippingid,
                  status, 
                  state,
                  state_datetime,
                  Row_number() over (partition by shippingid order by state_datetime DESC) as R
           FROM public.shipping
           ORDER BY orderid ASC,
                    shippingid ASC,
                    state_datetime ASC
            ),
sstart AS (SELECT shippingid,
                  state_datetime AS shipping_start_fact_datetime
           FROM public.shipping
           WHERE state= 'booked' 
           ORDER BY orderid ASC,
                    shippingid ASC,
                    state_datetime ASC
           ),
send AS (SELECT shippingid,
                state_datetime AS shipping_end_fact_datetime
         FROM public.shipping
         WHERE state= 'recieved' 
         ORDER BY orderid ASC,
                  shippingid ASC,
                  state_datetime ASC
        )
INSERT INTO public.shipping_status(shippingid,
                                   status,
                                   state,
                                   shipping_start_fact_datetime,
                                   shipping_end_fact_datetime)
SELECT s.shippingid,
       s.status,
       s.state,
       shipping_start_fact_datetime,
       shipping_end_fact_datetime
FROM s
LEFT JOIN sstart ON sstart.shippingid=s.shippingid
LEFT JOIN send ON send.shippingid=s.shippingid 
WHERE R=1;

--  Создайте представление shipping_datamart на основании готовых таблиц для аналитики и включите в него
--DROP VIEW IF EXISTS public.shipping_datamart;
--CREATE VIEW public.shipping_datamart AS
SELECT
si.shippingid,
vendorid,
transfer_type,
EXTRACT(day FROM AGE(shipping_end_fact_datetime,shipping_start_fact_datetime)) AS full_day_at_shipping,
CASE 
    WHEN shipping_end_fact_datetime > shipping_plan_datetime THEN 1
    ELSE 0
END AS is_delay,
CASE 
    WHEN status='finished' THEN 1
    ELSE 0
END AS is_shipping_finish,
CASE
    WHEN shipping_end_fact_datetime > shipping_plan_datetime THEN EXTRACT(day FROM AGE(shipping_end_fact_datetime,shipping_plan_datetime)) 
    ELSE 0
END AS delay_day_at_shipping,
payment_amount,
payment_amount*(shipping_country_base_rate + agreement_rate + shipping_transfer_rate) AS vat,
payment_amount*agreement_commission AS profit
FROM public.shipping_info AS si
LEFT JOIN public.shipping_transfer  AS st ON st.id = si.transfer_type_id 
LEFT JOIN public.shipping_status AS ss ON ss.shippingid =si.shippingid 
LEFT JOIN public.shipping_country_rates AS scr ON scr.id = si.shipping_country_id  
LEFT JOIN public.shipping_agreement AS sa ON sa.agreementid =si.agreementid;
```



