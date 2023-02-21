exports.natesInsertAndUpdates = `
drop table if exists schedule_a_meridian;
create table schedule_a_meridian
as
select distinct
  coalesce(
    substring(antspas."sys prin agt", 6, 4),
    crd."bnkid"
  ) as "bnkid_remap",
  crd."indicator",
  crd."level1",
  crd."level2",
  crd."merchantbillingcodename",
  cast(0 as float) as Volume_factor,
  cast(0 as float) as Amount_factor
from consolidated_residual_details as crd
  left outer join  account_no_to_sys_prin_agt_sales_rep as antspas
    on left(antspas."account no", 15) = crd.merchantnumber
order by
  coalesce(
    substring(antspas."sys prin agt", 6, 4),
    crd."bnkid"
  ),
  crd."indicator",
  crd."level1",
  crd."level2",
  crd."merchantbillingcodename";

update  schedule_a_meridian
set volume_factor = 0.0275::float
where (
  bnkid_remap = '2900'
  and indicator = 'EXPENSE'
  and level1 = 'PROCESSING EXPENSE'
  and level2 = 'BANKCARD TRANSACTIONS'
  and merchantbillingcodename in (
    'DISCOVER ACQUIRING', 'VISA', 'MASTERCARD'
  )
);



update  schedule_a_meridian
set amount_factor = 0.002::float
where (
  bnkid_remap = '2900'
  and indicator = 'EXPENSE'
  and level1 = 'PROCESSING EXPENSE'
  and level2 = 'BANKCARD TRANSACTIONS'
  and merchantbillingcodename in ('AMERICAN EXPRESS BLUE')
);

update  schedule_a_meridian
set amount_factor = 0.0003::float
where (
  bnkid_remap = '2900'
  and indicator = 'EXPENSE'
  and level1 = 'PROCESSING EXPENSE'
  and level2 = 'BANKCARD TRANSACTIONS'
  and merchantbillingcodename in (
    'DISCOVER ACQUIRING', 'VISA', 'MASTERCARD'
  )
);

update  schedule_a_meridian
set volume_factor = 9.95::float
where (
  bnkid_remap = '2900'
  and indicator = 'EXPENSE'
  and merchantbillingcodename in ('MERCHANTACCOUNT ON FILE')
);

update  schedule_a_meridian
set volume_factor = 0.02::float
where (
  bnkid_remap = '2900'
  and indicator = 'EXPENSE'
  and merchantbillingcodename in ('ETC BATCH HEADER')
);

update  schedule_a_meridian
set volume_factor = (20.00 - 12.00)::float
where (
  bnkid_remap = '2900'
  and indicator = 'EXPENSE'
  and merchantbillingcodename in (
    'ACQUIRER CHARGEBACK', 'ISSUER CHARGEBACK'
  )
);
`;

exports.selectResultsSum = (column, indicator) => {
  return `
select sum(f.${column})::float from (select
  to_char(cast(crd."date" as date), 'MM/DD/YYYY') as date,
  crd."bnkid",
  coalesce(
    substring(antspas."sys prin agt", 6, 4),
    crd."bnkid"
  ) as "bnkid_remap",
  crd."indicator",
  crd."yearmonth",
  crd."platformid",
  crd."merchantnumber",
  crd."merchantname",
  crd."level1",
  crd."level2",
  crd."level3",
  crd."element",
  crd."buyrate",
  crd."merchantbillingcode",
  crd."merchantbillingcodename",
  crd."revn_byrt_in",
  crd."clientormerchant_in",
  crd."volume",
  case
    when crd.indicator = 'EXPENSE' then -(sam.volume_factor::float)::float
    else sam.volume_factor::float
  end as volume_factor,
  case
    when crd.indicator = 'EXPENSE' then (-(crd."volume"::float) * sam.volume_factor::float)::float
    else (crd."volume"::float * sam.volume_factor::float)::float
  end as volume_cost,
  case
    when crd.indicator = 'EXPENSE' then -(crd.amount::float)::float
    else crd.amount::float
  end as amount,
  case
    when crd.indicator = 'EXPENSE' then -(sam.amount_factor::float)::float
    else sam.amount_factor::float
  end as amount_factor,
  case
    when crd.indicator = 'EXPENSE' then (-(crd."amount"::float) * sam.amount_factor)::float
    else (crd.amount::float * sam.amount_factor::float)::float
  end as amount_cost,
  case
    when crd.indicator = 'EXPENSE' then ((-(crd.amount::float)::float - (crd."volume"::float * sam.volume_factor::float)::float)::float - (crd."amount"::float * sam.amount_factor::float)::float)::float
    else ((crd.amount::float + (crd."volume"::float * sam.volume_factor::float)::float)::float + (crd."amount"::float * sam.amount_factor::float)::float)::float
  end as amount_after_cost
from consolidated_residual_details as crd
  left outer join account_no_to_sys_prin_agt_sales_rep as antspas
    on left(antspas."account no", 15) = crd.merchantnumber
  left outer join schedule_a_meridian as sam
    on (
      sam.bnkid_remap = coalesce(
        substring(antspas."sys prin agt", 6, 4),
        crd."bnkid"
      )
      and sam.indicator = crd.indicator
      and sam.level1 = crd.level1
      and sam.level2 = crd.level2
      and sam.merchantbillingcodename = crd.merchantbillingcodename
    )
where (
  1 = 1
  and coalesce(
    substring(antspas."sys prin agt", 6, 4),
    crd."bnkid"
  ) = '2900'
  and crd.Date >= to_char(cast((select date_trunc('month', current_date) - interval '1 month') as date), 'MM/DD/YYYY')::TIMESTAMP
)) as f
WHERE f.indicator = '${indicator}'
`;
};

exports.selectResultsFull = () => {
  return `
select
  to_char(cast(crd."date" as date), 'MM/DD/YYYY') as date,
  crd."bnkid",
  coalesce(
    substring(antspas."sys prin agt", 6, 4),
    crd."bnkid"
  ) as "bnkid_remap",
  crd."indicator",
  crd."yearmonth",
  crd."platformid",
  crd."merchantnumber",
  crd."merchantname",
  crd."level1",
  crd."level2",
  crd."level3",
  crd."element",
  crd."buyrate",
  crd."merchantbillingcode",
  crd."merchantbillingcodename",
  crd."revn_byrt_in",
  crd."clientormerchant_in",
  crd."volume",
  case
    when crd.indicator = 'EXPENSE' then -(sam.volume_factor::float)::float
    else sam.volume_factor::float
  end as volume_factor,
  case
    when crd.indicator = 'EXPENSE' then (-(crd."volume"::float) * sam.volume_factor::float)::float
    else (crd."volume"::float * sam.volume_factor::float)::float
  end as volume_cost,
  case
    when crd.indicator = 'EXPENSE' then -(crd.amount::float)::float
    else crd.amount::float
  end as amount,
  case
    when crd.indicator = 'EXPENSE' then -(sam.amount_factor::float)::float
    else sam.amount_factor::float
  end as amount_factor,
  case
    when crd.indicator = 'EXPENSE' then (-(crd."amount"::float) * sam.amount_factor)::float
    else (crd.amount::float * sam.amount_factor::float)::float
  end as amount_cost,
  case
    when crd.indicator = 'EXPENSE' then ((-(crd.amount::float)::float - (crd."volume"::float * sam.volume_factor::float)::float)::float - (crd."amount"::float * sam.amount_factor::float)::float)::float
    else ((crd.amount::float + (crd."volume"::float * sam.volume_factor::float)::float)::float + (crd."amount"::float * sam.amount_factor::float)::float)::float
  end as amount_after_cost
from consolidated_residual_details as crd
  left outer join account_no_to_sys_prin_agt_sales_rep as antspas
    on left(antspas."account no", 15) = crd.merchantnumber
  left outer join schedule_a_meridian as sam
    on (
      sam.bnkid_remap = coalesce(
        substring(antspas."sys prin agt", 6, 4),
        crd."bnkid"
      )
      and sam.indicator = crd.indicator
      and sam.level1 = crd.level1
      and sam.level2 = crd.level2
      and sam.merchantbillingcodename = crd.merchantbillingcodename
    )
where (
  1 = 1
  and coalesce(
    substring(antspas."sys prin agt", 6, 4),
    crd."bnkid"
  ) = '2900'
  and crd.Date >= to_char(cast((select date_trunc('month', current_date) - interval '1 month') as date), 'MM/DD/YYYY')::TIMESTAMP
)
`;
};
