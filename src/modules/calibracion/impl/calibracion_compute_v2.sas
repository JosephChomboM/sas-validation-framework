/* =========================================================================
calibracion_compute_v2.sas - Computo CAS-first del modulo Calibracion

Flujo:
- Input canonico unico en CAS con columna Split
- Resultados finales en CAS
- Staging minimo en work solo para mantener cortes numericos exactos
  de TRAIN via PROC RANK
- Sorting solo al final
========================================================================= */

%macro _cal_var_exists(data=, var=, outvar=_cal_exists);
    %local _dsid _rc _exists;
    %let _exists=0;

    %let _dsid=%sysfunc(open(&data.));
    %if &_dsid. > 0 %then %do;
        %if %sysfunc(varnum(&_dsid., &var.)) > 0 %then %let _exists=1;
        %let _rc=%sysfunc(close(&_dsid.));
    %end;

    %let &outvar=&_exists.;
%mend _cal_var_exists;

%macro _cal_push_unique(list_name=, value=);
    %local _cur _i _tok _found;
    %let _found=0;
    %let _cur=&&&list_name.;

    %if %length(%superq(value))=0 %then %return;

    %let _i=1;
    %let _tok=%scan(%superq(_cur), &_i., %str( ));
    %do %while(%length(%superq(_tok)) > 0);
        %if %upcase(%superq(_tok))=%upcase(%superq(value)) %then %do;
            %let _found=1;
            %goto _cal_push_unique_done;
        %end;
        %let _i=%eval(&_i. + 1);
        %let _tok=%scan(%superq(_cur), &_i., %str( ));
    %end;

    %if &_found.=0 %then %do;
        %if %length(%superq(_cur))=0 %then %let &list_name=%superq(value);
        %else %let &list_name=%superq(_cur) %superq(value);
    %end;

%_cal_push_unique_done:
    %let &list_name=%sysfunc(compbl(&&&list_name.));
%mend _cal_push_unique;

%macro _cal_drop_cas(name=);
    %if %length(%superq(name))=0 %then %return;
    proc cas;
        session conn;
        table.dropTable / caslib="casuser" name="&name." quiet=true;
    quit;
%mend _cal_drop_cas;

%macro _cal_append_cas(source=, target=);
    %if %length(%superq(source))=0 or %length(%superq(target))=0 %then %return;
    proc cas;
        session conn;
        table.append /
            source={caslib="casuser", name="&source."},
            target={caslib="casuser", name="&target."};
    quit;
%mend _cal_append_cas;

%macro _cal_sort_cas(table_name=, orderby=, groupby={});
    %if %length(%superq(table_name))=0 or %length(%superq(orderby))=0 %then
        %return;

    proc cas;
        session conn;
        table.partition /
            table={
                caslib="casuser",
                name="&table_name.",
                orderby=&orderby.,
                groupby=&groupby.
            },
            casout={
                caslib="casuser",
                name="&table_name.",
                replace=true
            };
    quit;
%mend _cal_sort_cas;

%macro _cal_init_outputs(detail=casuser._cal_detail, cuts=casuser._cal_cuts);

    data &detail.;
        length Var_Seq 8 Variable $64 Var_Type $3 Split $32 Calc_Mode $3
            Bucket_Order 8 Bucket_Label $200 N_Cuentas 8 Pct_Cuentas 8
            Registros_RD 8 Registros_PD 8 Registros_RD_Pond 8
            Registros_PD_Pond 8 RD 8 PD 8 LI_10 8 LS_10 8 LI_25 8 LS_25 8;
        format Pct_Cuentas RD PD LI_10 LS_10 LI_25 LS_25 percent8.2;
        stop;
    run;

    data &cuts.;
        length Var_Seq 8 Variable $64 Var_Type $3 Source_Split $32
            Bucket_Order 8 Bucket_Label $200 Inicio 8 Fin 8 Flag_Ini 8
            Flag_Fin 8;
        format Inicio Fin best12.4;
        stop;
    run;

%mend _cal_init_outputs;

%macro _cal_vasicek(datain=, est=PD, rho=0.005, alpha=0.10);
    %local _n;
    %let _n=%sysfunc(int(%sysevalf(100*&alpha.)));

    data &datain.;
        set &datain.;
        format LI_&_n. percent8.2 LS_&_n. percent8.2;

        if missing(&est.) then do;
            LI_&_n.=.;
            LS_&_n.=.;
        end;
        else do;
            _arg1=((1-&rho.)**(-0.5)) * quantile("NORMAL", &est.);
            _arg2=((&rho./(1-&rho.))**0.5) * quantile("NORMAL", &alpha.);
            _arg3=((&rho./(1-&rho.))**0.5) * quantile("NORMAL", 1-&alpha.);
            LI_&_n.=cdf("NORMAL", _arg1 + _arg2);
            LS_&_n.=cdf("NORMAL", _arg1 + _arg3);
        end;

        drop _arg1 _arg2 _arg3;
    run;
%mend _cal_vasicek;

%macro _cal_build_driver_meta(data=, vars_num=, vars_cat=,
    out=work._cal_driver_meta, out_keep=_cal_keep, out_any=_cal_any);

    %local _var_seq _keep _any;
    %let _var_seq=0;
    %let _keep=;
    %let _any=0;

    data &out.;
        length Var_Seq 8 Variable $64 Var_Type $3;
        stop;
    run;

    %macro _cal_register(list=, type=);
        %local _idx _drv _exists_input;
        %let _idx=1;
        %let _drv=%scan(%superq(list), &_idx., %str( ));

        %do %while(%length(%superq(_drv)) > 0);
            %let _var_seq=%eval(&_var_seq. + 1);
            %_cal_var_exists(data=&data., var=&_drv., outvar=_exists_input);

            %if &_exists_input.=0 %then %do;
                %put WARNING: [calibracion_compute] Driver &_drv. no existe
                    en input y sera omitido.;
            %end;
            %else %do;
                %let _any=1;
                %let _keep=&_keep. &_drv.;

                data work._cal_driver_row;
                    length Variable $64 Var_Type $3;
                    Var_Seq=&_var_seq.;
                    Variable="&_drv.";
                    Var_Type="&type.";
                    output;
                run;

                proc append base=&out. data=work._cal_driver_row force;
                run;
            %end;

            %let _idx=%eval(&_idx. + 1);
            %let _drv=%scan(%superq(list), &_idx., %str( ));
        %end;
    %mend _cal_register;

    %if %length(%superq(vars_num)) > 0 %then %_cal_register(list=&vars_num., type=NUM);
    %if %length(%superq(vars_cat)) > 0 %then %_cal_register(list=&vars_cat., type=CAT);

    proc datasets library=work nolist nowarn;
        delete _cal_driver_row;
    quit;

    %let &out_keep=%sysfunc(compbl(&_keep.));
    %let &out_any=&_any.;
%mend _cal_build_driver_meta;

%macro _cal_prepare_numeric(data=, var=, out=);
    data &out.;
        set &data.;
        if &var. in (., 1111111111, -1111111111, 2222222222, -2222222222,
            3333333333, -3333333333, 4444444444, 5555555555, 6666666666,
            7777777777, -999999999) then &var.=.;
    run;
%mend _cal_prepare_numeric;

%macro _cal_build_numeric_cuts(data=, var=, variable=, var_seq=, groups=5,
    out=work._cal_var_cuts);

    %local _grp _n_valid;
    %let _grp=&groups.;
    %if %length(%superq(_grp))=0 %then %let _grp=5;
    %if %sysevalf(&_grp. < 1) %then %let _grp=5;

    %let _n_valid=0;
    proc sql noprint;
        select count(*) into :_n_valid trimmed
        from &data.
        where not missing(&var.);
    quit;

    data work._cal_cut_missing;
        length Variable $64 Var_Type $3 Source_Split $32 Bucket_Label $200;
        Var_Seq=&var_seq.;
        Variable="&variable.";
        Var_Type="NUM";
        Source_Split="TRAIN";
        Bucket_Order=0;
        Bucket_Label="00. Missing";
        Inicio=.;
        Fin=.;
        Flag_Ini=0;
        Flag_Fin=0;
        format Inicio Fin best12.4;
        output;
    run;

    %if &_n_valid.=0 %then %do;
        data &out.;
            set work._cal_cut_missing;
        run;
        %return;
    %end;

    proc rank data=&data.(keep=&var. where=(not missing(&var.)))
        out=work._cal_rank_tmp groups=&_grp.;
        var &var.;
        ranks _cal_rango_ini;
    run;

    proc sql;
        create table work._cal_bins_tmp as
        select _cal_rango_ini as rango_ini,
               min(&var.) as minval,
               max(&var.) as maxval
        from work._cal_rank_tmp
        group by _cal_rango_ini
        order by _cal_rango_ini;
    quit;

    data work._cal_cut_bins;
        set work._cal_bins_tmp end=eof;
        length Variable $64 Var_Type $3 Source_Split $32 Bucket_Label $200;
        retain prev_fin .;

        Var_Seq=&var_seq.;
        Variable="&variable.";
        Var_Type="NUM";
        Source_Split="TRAIN";
        Bucket_Order=rango_ini + 1;
        Inicio=prev_fin;
        Fin=maxval;
        Flag_Ini=(_n_ = 1);
        Flag_Fin=eof;
        if Flag_Ini then Inicio=.;

        if Flag_Ini then
            Bucket_Label=cats(put(Bucket_Order, z2.), ". <-Inf; ",
                strip(put(Fin, best12.4)), "]");
        else if Flag_Fin then
            Bucket_Label=cats(put(Bucket_Order, z2.), ". <",
                strip(put(Inicio, best12.4)), "; +Inf>");
        else
            Bucket_Label=cats(put(Bucket_Order, z2.), ". <",
                strip(put(Inicio, best12.4)), "; ",
                strip(put(Fin, best12.4)), "]");

        prev_fin=Fin;
        format Inicio Fin best12.4;
        keep Var_Seq Variable Var_Type Source_Split Bucket_Order Bucket_Label
            Inicio Fin Flag_Ini Flag_Fin;
    run;

    data &out.;
        set work._cal_cut_missing work._cal_cut_bins;
    run;

    proc sort data=&out.;
        by Bucket_Order;
    run;

    proc datasets library=work nolist nowarn;
        delete _cal_rank_tmp _cal_bins_tmp _cal_cut_bins _cal_cut_missing;
    quit;
%mend _cal_build_numeric_cuts;

%macro _cal_metric_detail(data=, split_var=Split, variable=, var_seq=,
    var_type=, calc_mode=STD, target=Target_Value, score=Score_Value,
    weight_var=Weight_Value, out=casuser._cal_detail_var);

    %_cal_drop_cas(name=_cal_metric_totals);
    %_cal_drop_cas(name=_cal_metric_base);

    proc fedsql sessref=conn;
        create table casuser._cal_metric_totals {options replace=true} as
        select &split_var.,
               count(*) as Total_Cuentas
        from &data.
        group by &split_var.;
    quit;

    %if %upcase(&calc_mode.)=WGT %then %do;
        proc fedsql sessref=conn;
            create table casuser._cal_metric_base {options replace=true} as
            select a.&split_var. as Split,
                   a.Bucket_Order,
                   a.Bucket_Label,
                   count(*) as N_Cuentas,
                   case
                       when max(t.Total_Cuentas) > 0
                       then count(*) * 1.0 / max(t.Total_Cuentas)
                       else .
                   end as Pct_Cuentas,
                   sum(case when a.&target. is null then 0 else 1 end)
                       as Registros_RD,
                   sum(case when a.&score. is null then 0 else 1 end)
                       as Registros_PD,
                   sum(case when a.&target. is null or a.&weight_var. is null
                       then 0 else 1 end) as Registros_RD_Pond,
                   sum(case when a.&score. is null or a.&weight_var. is null
                       then 0 else 1 end) as Registros_PD_Pond,
                   case
                       when sum(case when a.&target. is null or
                           a.&weight_var. is null then 0
                           else a.&weight_var. end) > 0
                       then sum(case when a.&target. is null or
                           a.&weight_var. is null then 0
                           else a.&target. * a.&weight_var. end)
                            / sum(case when a.&target. is null or
                           a.&weight_var. is null then 0
                           else a.&weight_var. end)
                       else .
                   end as RD,
                   case
                       when sum(case when a.&score. is null or
                           a.&weight_var. is null then 0
                           else a.&weight_var. end) > 0
                       then sum(case when a.&score. is null or
                           a.&weight_var. is null then 0
                           else a.&score. * a.&weight_var. end)
                            / sum(case when a.&score. is null or
                           a.&weight_var. is null then 0
                           else a.&weight_var. end)
                       else .
                   end as PD
            from &data. a
            left join casuser._cal_metric_totals t
              on a.&split_var. = t.&split_var.
            group by a.&split_var., a.Bucket_Order, a.Bucket_Label;
        quit;
    %end;
    %else %do;
        proc fedsql sessref=conn;
            create table casuser._cal_metric_base {options replace=true} as
            select a.&split_var. as Split,
                   a.Bucket_Order,
                   a.Bucket_Label,
                   count(*) as N_Cuentas,
                   case
                       when max(t.Total_Cuentas) > 0
                       then count(*) * 1.0 / max(t.Total_Cuentas)
                       else .
                   end as Pct_Cuentas,
                   sum(case when a.&target. is null then 0 else 1 end)
                       as Registros_RD,
                   sum(case when a.&score. is null then 0 else 1 end)
                       as Registros_PD,
                   cast(null as double) as Registros_RD_Pond,
                   cast(null as double) as Registros_PD_Pond,
                   case
                       when sum(case when a.&target. is null then 0 else 1 end)
                           > 0
                       then sum(case when a.&target. is null then 0
                           else a.&target. end)
                            / sum(case when a.&target. is null then 0 else 1 end)
                       else .
                   end as RD,
                   case
                       when sum(case when a.&score. is null then 0 else 1 end)
                           > 0
                       then sum(case when a.&score. is null then 0
                           else a.&score. end)
                            / sum(case when a.&score. is null then 0 else 1 end)
                       else .
                   end as PD
            from &data. a
            left join casuser._cal_metric_totals t
              on a.&split_var. = t.&split_var.
            group by a.&split_var., a.Bucket_Order, a.Bucket_Label;
        quit;
    %end;

    data &out.;
        length Variable $64 Var_Type $3 Split $32 Calc_Mode $3;
        set casuser._cal_metric_base;
        Var_Seq=&var_seq.;
        Variable="&variable.";
        Var_Type="&var_type.";
        Calc_Mode="%upcase(&calc_mode.)";
        format Pct_Cuentas RD PD LI_10 LS_10 LI_25 LS_25 percent8.2;
    run;

    %_cal_vasicek(datain=&out., est=PD, rho=0.005, alpha=0.10);
    %_cal_vasicek(datain=&out., est=PD, rho=0.005, alpha=0.25);

    %_cal_drop_cas(name=_cal_metric_totals);
    %_cal_drop_cas(name=_cal_metric_base);
%mend _cal_metric_detail;

%macro _cal_finalize_detail_bucket_order(detail_table=casuser._cal_detail,
    out_table=casuser._cal_detail_stage);

    %_cal_drop_cas(name=_cal_cat_order_src);
    %_cal_drop_cas(name=_cal_cat_order);

    proc fedsql sessref=conn;
        create table casuser._cal_cat_order_src {options replace=true} as
        select distinct Var_Seq,
               Variable,
               Bucket_Label
        from &detail_table.
        where upcase(Var_Type)='CAT';
    quit;

    data work._cal_cat_order;
        set casuser._cal_cat_order_src;
        length _bucket_sort 8;
        if upcase(Bucket_Label)='00. MISSING' then _bucket_sort=-1;
        else if upcase(Bucket_Label)='99. OUTSIDE TRAIN CUTS' then
            _bucket_sort=999998;
        else _bucket_sort=0;
    run;

    proc sort data=work._cal_cat_order;
        by Var_Seq _bucket_sort Bucket_Label;
    run;

    data work._cal_cat_order;
        set work._cal_cat_order;
        by Var_Seq;
        retain Bucket_Order;
        if first.Var_Seq then Bucket_Order=0;
        else Bucket_Order+1;
        keep Var_Seq Bucket_Label Bucket_Order;
    run;

    data casuser._cal_cat_order;
        set work._cal_cat_order;
    run;

    proc fedsql sessref=conn;
        create table &out_table. {options replace=true} as
        select a.Var_Seq,
               a.Variable,
               a.Var_Type,
               a.Split,
               a.Calc_Mode,
               case
                   when upcase(a.Var_Type)='CAT'
                       then coalesce(b.Bucket_Order, a.Bucket_Order)
                   else a.Bucket_Order
               end as Bucket_Order,
               a.Bucket_Label,
               a.N_Cuentas,
               a.Pct_Cuentas,
               a.Registros_RD,
               a.Registros_PD,
               a.Registros_RD_Pond,
               a.Registros_PD_Pond,
               a.RD,
               a.PD,
               a.LI_10,
               a.LS_10,
               a.LI_25,
               a.LS_25
        from &detail_table. a
        left join casuser._cal_cat_order b
          on a.Var_Seq=b.Var_Seq
         and a.Bucket_Label=b.Bucket_Label;
    quit;

    proc datasets library=work nolist nowarn;
        delete _cal_cat_order;
    quit;

    %_cal_drop_cas(name=_cal_cat_order_src);
    %_cal_drop_cas(name=_cal_cat_order);
%mend _cal_finalize_detail_bucket_order;

%macro _cal_compute_numeric_one(input_data=, split_var=Split, variable=,
    var_seq=, target=, score_var=, weight_var=, groups=5, calc_weighted=0,
    detail_target=_cal_detail, cuts_target=_cal_cuts);

    %_cal_drop_cas(name=_cal_num_input);
    %_cal_drop_cas(name=_cal_var_cuts);
    %_cal_drop_cas(name=_cal_tagged);
    %_cal_drop_cas(name=_cal_detail_var);
    %_cal_drop_cas(name=_cal_detail_var_w);

    data casuser._cal_num_input;
        set &input_data.(keep=&split_var. &target. &score_var.
            %if %length(%superq(weight_var)) > 0 %then &weight_var.;
            &variable.);
        length Split $32;
        Split=&split_var.;
        Target_Value=&target.;
        Score_Value=&score_var.;
        %if %length(%superq(weight_var)) > 0 %then %do;
            Weight_Value=&weight_var.;
        %end;
        %else %do;
            Weight_Value=.;
        %end;
        Driver_Value=&variable.;
        if Driver_Value in (., 1111111111, -1111111111, 2222222222,
            -2222222222, 3333333333, -3333333333, 4444444444, 5555555555,
            6666666666, 7777777777, -999999999) then Driver_Value=.;
        keep Split Target_Value Score_Value Weight_Value Driver_Value;
    run;

    data work._cal_train_num_src;
        set casuser._cal_num_input(where=(upcase(Split)='TRAIN')
            keep=Driver_Value);
    run;

    %_cal_build_numeric_cuts(data=work._cal_train_num_src, var=Driver_Value,
        variable=&variable., var_seq=&var_seq., groups=&groups.,
        out=work._cal_var_cuts);

    data casuser._cal_var_cuts;
        set work._cal_var_cuts;
    run;

    proc fedsql sessref=conn;
        create table casuser._cal_tagged {options replace=true} as
        select a.*,
               case
                   when a.Driver_Value is null then 0
                   when b.Bucket_Order is null then 999
                   else b.Bucket_Order
               end as Bucket_Order,
               case
                   when a.Driver_Value is null then '00. Missing'
                   when b.Bucket_Label is null then '99. Outside TRAIN Cuts'
                   else b.Bucket_Label
               end as Bucket_Label
        from casuser._cal_num_input a
        left join casuser._cal_var_cuts b
          on b.Bucket_Order > 0
         and a.Driver_Value is not null
         and (
                (b.Flag_Ini = 1 and a.Driver_Value <= b.Fin)
             or (b.Flag_Fin = 1 and a.Driver_Value > b.Inicio)
             or (b.Flag_Ini = 0 and b.Flag_Fin = 0 and
                 a.Driver_Value > b.Inicio and a.Driver_Value <= b.Fin)
         );
    quit;

    %_cal_metric_detail(data=casuser._cal_tagged, split_var=Split,
        variable=&variable., var_seq=&var_seq., var_type=NUM, calc_mode=STD,
        target=Target_Value, score=Score_Value, weight_var=Weight_Value,
        out=casuser._cal_detail_var);
    %_cal_append_cas(source=_cal_detail_var, target=&detail_target.);
    %_cal_append_cas(source=_cal_var_cuts, target=&cuts_target.);

    %if &calc_weighted.=1 %then %do;
        %_cal_metric_detail(data=casuser._cal_tagged, split_var=Split,
            variable=&variable., var_seq=&var_seq., var_type=NUM,
            calc_mode=WGT, target=Target_Value, score=Score_Value,
            weight_var=Weight_Value, out=casuser._cal_detail_var_w);
        %_cal_append_cas(source=_cal_detail_var_w, target=&detail_target.);
    %end;

    proc datasets library=work nolist nowarn;
        delete _cal_train_num_src _cal_var_cuts;
    quit;

    %_cal_drop_cas(name=_cal_num_input);
    %_cal_drop_cas(name=_cal_var_cuts);
    %_cal_drop_cas(name=_cal_tagged);
    %_cal_drop_cas(name=_cal_detail_var);
    %_cal_drop_cas(name=_cal_detail_var_w);
%mend _cal_compute_numeric_one;

%macro _cal_compute_categorical_one(input_data=, split_var=Split, variable=,
    var_seq=, target=, score_var=, weight_var=, calc_weighted=0,
    detail_target=_cal_detail);

    %_cal_drop_cas(name=_cal_tagged);
    %_cal_drop_cas(name=_cal_detail_var);
    %_cal_drop_cas(name=_cal_detail_var_w);

    data casuser._cal_tagged;
        set &input_data.(keep=&split_var. &target. &score_var.
            %if %length(%superq(weight_var)) > 0 %then &weight_var.;
            &variable.);
        length Split $32 Bucket_Label $200;
        Split=&split_var.;
        Target_Value=&target.;
        Score_Value=&score_var.;
        %if %length(%superq(weight_var)) > 0 %then %do;
            Weight_Value=&weight_var.;
        %end;
        %else %do;
            Weight_Value=.;
        %end;
        Bucket_Label=strip(vvaluex("&variable."));
        if missing(Bucket_Label) then Bucket_Label="00. Missing";
        keep Split Target_Value Score_Value Weight_Value Bucket_Label;
    run;

    %_cal_metric_detail(data=casuser._cal_tagged, split_var=Split,
        variable=&variable., var_seq=&var_seq., var_type=CAT, calc_mode=STD,
        target=Target_Value, score=Score_Value, weight_var=Weight_Value,
        out=casuser._cal_detail_var);
    %_cal_append_cas(source=_cal_detail_var, target=&detail_target.);

    %if &calc_weighted.=1 %then %do;
        %_cal_metric_detail(data=casuser._cal_tagged, split_var=Split,
            variable=&variable., var_seq=&var_seq., var_type=CAT,
            calc_mode=WGT, target=Target_Value, score=Score_Value,
            weight_var=Weight_Value, out=casuser._cal_detail_var_w);
        %_cal_append_cas(source=_cal_detail_var_w, target=&detail_target.);
    %end;

    %_cal_drop_cas(name=_cal_tagged);
    %_cal_drop_cas(name=_cal_detail_var);
    %_cal_drop_cas(name=_cal_detail_var_w);
%mend _cal_compute_categorical_one;

%macro _calibration_compute(input_data=, split_var=Split, driver_meta=,
    target=, score_var=, weight_var=, groups=5, calc_weighted=0,
    out_detail=casuser._cal_detail, out_cuts=casuser._cal_cuts);

    %local _cal_nvars _i _var _type _seq _detail_mem _cuts_mem;
    %let _cal_nvars=0;
    %let _detail_mem=%scan(&out_detail., 2, .);
    %let _cuts_mem=%scan(&out_cuts., 2, .);

    %_cal_init_outputs(detail=&out_detail., cuts=&out_cuts.);

    data _null_;
        set &driver_meta. end=eof;
        call symputx(cats("_cal_var", _n_), Variable, "L");
        call symputx(cats("_cal_type", _n_), Var_Type, "L");
        call symputx(cats("_cal_seq", _n_), Var_Seq, "L");
        if eof then call symputx("_cal_nvars", _n_, "L");
    run;

    %if %length(%superq(_cal_nvars))=0 %then %let _cal_nvars=0;

    %do _i=1 %to &_cal_nvars.;
        %let _var=&&_cal_var&_i.;
        %let _type=&&_cal_type&_i.;
        %let _seq=&&_cal_seq&_i.;

        %put NOTE: [calibracion_compute] Procesando &_type. &_var.
            (Var_Seq=&_seq.).;

        %if %upcase(&_type.)=NUM %then %do;
            %_cal_compute_numeric_one(input_data=&input_data.,
                split_var=&split_var., variable=&_var., var_seq=&_seq.,
                target=&target., score_var=&score_var.,
                weight_var=&weight_var., groups=&groups.,
                calc_weighted=&calc_weighted., detail_target=&_detail_mem.,
                cuts_target=&_cuts_mem.);
        %end;
        %else %do;
            %_cal_compute_categorical_one(input_data=&input_data.,
                split_var=&split_var., variable=&_var., var_seq=&_seq.,
                target=&target., score_var=&score_var.,
                weight_var=&weight_var., calc_weighted=&calc_weighted.,
                detail_target=&_detail_mem.);
        %end;
    %end;

    %_cal_finalize_detail_bucket_order(detail_table=&out_detail.,
        out_table=casuser._cal_detail_stage);

    data &out_detail.;
        set casuser._cal_detail_stage;
    run;

    %_cal_drop_cas(name=_cal_detail_stage);

    %_cal_sort_cas(table_name=&_detail_mem.,
        orderby=%str({"Var_Seq", "Split", "Calc_Mode", "Bucket_Order",
        "Bucket_Label"}));
    %_cal_sort_cas(table_name=&_cuts_mem.,
        orderby=%str({"Var_Seq", "Bucket_Order", "Bucket_Label"}));

%mend _calibration_compute;
