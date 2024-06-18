--Dynamic date code
/*
!NOTE!
- Must used current_date() and run at first day of month, if done experiment
- Now using '2023-12-31' instead of current_date() as date_update_segment

*/

create or replace table `cdg-mark-cust-prd.TEMP_NUTCHAPONG.rfm_2023_status` as
with cal_nel_mtbp as
(select sa.MEMBER_NUMBER
--NEL Metric--
        , sum(case when SALE_DATE between date_add('2023-12-31', INTERVAL -1 year)+1 and '2023-12-31' then NET_SALES_AMT_INC_VAT else 0 end) sales_tp
        , sum(case when SALE_DATE between date_add('2023-12-31', INTERVAL -2 year)+1 and date_add('2023-12-31', INTERVAL -1 year) then NET_SALES_AMT_INC_VAT else 0 end) sales_lp
--Activity Metric--
        , date_diff('2023-12-31', max(sa.SALE_DATE),day) recency
        , case when count(distinct sa.SALE_DATE) = 1 then 0
          else date_diff(max(sa.SALE_DATE),min(sa.SALE_DATE),day)/(count(distinct sa.SALE_DATE)- 1) end as mtbp
--1-Timer
        , count(distinct sa.receipt_no) ticket
from `cdg-mark-cust-prd.TEMP_NUTCHAPONG.rfm_dummy_21_23_100624` sa
where 1=1
  and SALE_DATE between date_add('2023-12-31', INTERVAL -2 year)+1 and '2023-12-31'
group by 1
)

, nel_stat as
(select member_number
      , case when sales_lp > 0 and sales_tp > 0 then 'regular'
        when sales_lp <= 0 and sales_tp > 0 then 'new'
        when sales_lp > 0 and sales_tp <= 0 then 'lapsed'
        else 'returned' end as nel_status
from cal_nel_mtbp
)

--create new column for new shopping_cycle for manual CRM(20-30 days)--
, cal_activity as
(select t1.member_number
      , nel_status
      , case when t1.recency < cycle_days_weight then 'faster than cycle'
          when t1.recency between cycle_days_weight and cycle_days_weight * 1.1 then 'equal cycle'
          when t1.recency > cycle_days_weight * 1.1 then 'longer than cycle'
          when cycle_days_weight is null then 'first purchase'
          else 'n/a' end as behavior_shopping_cycle
      , case when nel_status in ('new','regular') and t1.recency <= cycle_days_weight then 'a.active'
          when nel_status in ('new','regular') and t1.recency > cycle_days_weight then 'b.churning'
          when nel_status in ('regular','lapsed') and t1.recency > cycle_days_weight then 'b.churning'
          when t1.recency < 180 then 'b.churning'
          when nel_status in ('lapsed') then 'c.lapsed'
          when t1.recency >= 180 and nel_status in ('new') then 'b.churning'
          when t1.recency >= 180 and nel_status in ('regular') then 'b.churning'
          when t1.recency >= 365 then 'c.lapsed'
          else 'n/a' end as activity_segment
      , ticket
      , sales_tp
from cal_nel_mtbp t1
join nel_stat
  using(member_number)
left join `cdg-mark-cust-prd.CAS_DS_DATABASE.ca_ds_customer_shopping_cycle_monthly` sc
  on t1.member_number = sc.member_number
where 1=1
  and  nel_status not in ('returned')
  and sc.date_update_segment in (select max(date_update_segment) from `cdg-mark-cust-prd.CAS_DS_DATABASE.ca_ds_customer_shopping_cycle_monthly`)
)

, cal_rfm_seg as
(select *
      , case when activity_segment in ('a.active') and ticket = 1 then 'a4.newbies'
        when activity_segment in ('a.active') and ticket > 1 and sales_tp >= 60000 then 'a1.champion'
        when activity_segment in ('a.active') and ticket > 1 and sales_tp >= 6000 then 'a2.loyal'
        when activity_segment in ('a.active') and ticket > 1 and sales_tp < 6000 then 'a3.potential'

        when activity_segment in ('b.churning') and ticket = 1 then 'b4.trier'
        when activity_segment in ('b.churning') and ticket > 1 and sales_tp >= 60000 then 'b1.cannot lose '
        when activity_segment in ('b.churning') and ticket > 1 and sales_tp >= 6000 then 'b2.need attention'
        when activity_segment in ('b.churning') and ticket > 1 and sales_tp < 6000 then 'b3.at risk'

        when activity_segment in ('c.lapsed') and ticket = 1 then 'c2.one-timer'
        when activity_segment in ('c.lapsed') and ticket > 1 and sales_tp < 6000 then 'c1.lost'

        else 'd.other' end as rfm_segment
        , '2023-12-31' as date_update_segment
from cal_activity
)

select *
from cal_rfm_seg
;

--agg table to analyze distribution
select nel_status
      , activity_segment
      , behavior_shopping_cycle
      , rfm_segment
      , count(distinct member_number) customer
from cal_rfm_seg
group by 1,2,3,4
order by 1,2,3,4
;


--Performance recheck--
--Part1: Overall performance
select rfm.nel_status
      , rfm.activity_segment
      , rfm.behavior_shopping_cycle
      , rfm.rfm_segment
      , count(distinct sa.member_number) customer
      , sum(sa.NET_SALES_AMT_INC_VAT) sales
      , count(distinct sa.SALE_DATE || sa.member_number) visit
      , count(distinct sa.receipt_no) ticket
from `cdg-mark-cust-prd.TEMP_NUTCHAPONG.rfm_dummy_21_23_100624` sa  --sales
join `cdg-mark-cust-prd.TEMP_NUTCHAPONG.rfm_2023_status` rfm
  on sa.member_number = rfm.member_number
where 1=1
  and extract(year from sa.SALE_DATE) in (2023,2022)
  and sa.member_number is not null
group by 1,2,3,4
;

--Part2: Penetration by dept
select rfm.nel_status
      , rfm.activity_segment
      , rfm.behavior_shopping_cycle
      , rfm.rfm_segment
      , department
      , count(distinct sa.member_number) customer
      , sum(sa.NET_SALES_AMT_INC_VAT) sales
      , count(distinct sa.SALE_DATE || sa.member_number) visit
      , count(distinct sa.receipt_no) ticket
from `cdg-mark-cust-prd.TEMP_NUTCHAPONG.rfm_dummy_21_23_100624` sa
, unnest([department,'Total']) as department
join `cdg-mark-cust-prd.TEMP_NUTCHAPONG.rfm_2023_status` rfm
  on sa.member_number = rfm.member_number
where 1=1
  and extract(year from sa.SALE_DATE) in (2023,2022)
  and sa.member_number is not null
group by 1,2,3,4,5
;
