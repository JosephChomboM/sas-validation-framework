/* =========================================================================
calibracion_compute.sas - Computo del modulo Calibracion (METOD8)

Backtesting por driver:
- Numericas: cortes TRAIN via PROC RANK y reutilizacion exacta en OOT
- Categoricas: agrupacion directa por valor formateado
- Metricas STD: N, % cuentas, RD, PD
- Metricas WGT: RD/PD ponderados por monto + conteos ponderables
- Bandas Vasicek: alpha 10% y 25%, rho 0.005

Pattern B:
- Copia TRAIN/OOT desde CAS a work
- Calcula buckets y metricas en work
- Publica resultados finales de vuelta a CAS
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

%macro _cal_init_outputs(detail=work._cal_detail_all,
    cuts=work._cal_cuts_all);

    data &detail.;
        length Var_Seq 8 Variable $64 Var_Type $3 Split $5 Calc_Mode $3
            Bucket_Order 8 Bucket_Label $200 N_Cuentas 8 Pct_Cuentas 8
            Registros_RD 8 Registros_PD 8 Registros_RD_Pond 8
            Registros_PD_Pond 8 RD 8 PD 8 LI_10 8 LS_10 8 LI_25 8 LS_25 8;
        format Pct_Cuentas RD PD LI_10 LS_10 LI_25 LS_25 percent8.2;
        stop;
    run;

    data &cuts.;
        length Var_Seq 8 Variable $64 Var_Type $3 Source_Split $5
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

%macro _cal_build_driver_meta(data_train=, data_oot=, vars_num=, vars_cat=,
    out=work._cal_driver_meta, out_keep_train=_cal_keep_train,
    out_keep_oot=_cal_keep_oot, out_any_train=_cal_any_train);

    %local _var_seq _keep_train _keep_oot _any_train;
    %let _var_seq=0;
    %let _keep_train=;
    %let _keep_oot=;
    %let _any_train=0;

    data &out.;
        length Var_Seq 8 Variable $64 Var_Type $3 In_Train 8 In_OOT 8;
        stop;
    run;

    %macro _cal_register(list=, type=);
        %local _idx _drv _exists_train _exists_oot;
        %let _idx=1;
        %let _drv=%scan(%superq(list), &_idx., %str( ));

        %do %while(%length(%superq(_drv)) > 0);
            %let _var_seq=%eval(&_var_seq. + 1);

            %_cal_var_exists(data=&data_train., var=&_drv.,
                outvar=_exists_train);
            %_cal_var_exists(data=&data_oot., var=&_drv.,
                outvar=_exists_oot);

            %if &_exists_train.=0 %then %do;
                %put WARNING: [calibracion_compute] Driver &_drv. no existe
                    en TRAIN y sera omitido.;
                %if &_exists_oot.=1 %then %do;
                    %put WARNING: [calibracion_compute] Driver &_drv. existe
                        en OOT pero no en TRAIN; no se calculara.;
                %end;
            %end;
            %else %do;
                %let _any_train=1;
                %let _keep_train=&_keep_train. &_drv.;

                %if &_exists_oot.=1 %then %let _keep_oot=&_keep_oot. &_drv.;
                %else %do;
                    %put WARNING: [calibracion_compute] Driver &_drv. existe
                        en TRAIN pero no en OOT; se calculara solo TRAIN.;
                %end;

                data work._cal_driver_row;
                    length Variable $64 Var_Type $3;
                    Var_Seq=&_var_seq.;
                    Variable="&_drv.";
                    Var_Type="&type.";
                    In_Train=&_exists_train.;
                    In_OOT=&_exists_oot.;
                    output;
                run;

                proc append base=&out. data=work._cal_driver_row force;
                run;
            %end;

            %let _idx=%eval(&_idx. + 1);
            %let _drv=%scan(%superq(list), &_idx., %str( ));
        %end;
    %mend _cal_register;

    %if %length(%superq(vars_num)) > 0 %then %do;
        %_cal_register(list=&vars_num., type=NUM);
    %end;

    %if %length(%superq(vars_cat)) > 0 %then %do;
        %_cal_register(list=&vars_cat., type=CAT);
    %end;

    proc datasets library=work nolist nowarn;
        delete _cal_driver_row;
    quit;

    %let &out_keep_train=%sysfunc(compbl(&_keep_train.));
    %let &out_keep_oot=%sysfunc(compbl(&_keep_oot.));
    %let &out_any_train=&_any_train.;

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
        length Variable $64 Var_Type $3 Source_Split $5 Bucket_Label $200;
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
        length Variable $64 Var_Type $3 Source_Split $5 Bucket_Label $200;
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

%macro _cal_apply_numeric_cuts(data=, var=, cuts=, out=);
    proc sql;
        create table &out. as
        select a.*,
               case
                   when missing(a.&var.) then 0
                   when missing(b.Bucket_Order) then 999
                   else b.Bucket_Order
               end as Bucket_Order,
               case
                   when missing(a.&var.) then "00. Missing"
                   when missing(b.Bucket_Label) then "99. Outside TRAIN Cuts"
                   else b.Bucket_Label
               end as Bucket_Label length=200
        from &data. as a
        left join &cuts.(where=(Bucket_Order > 0)) as b
          on not missing(a.&var.)
         and (
                (b.Flag_Ini = 1 and a.&var. <= b.Fin)
             or (b.Flag_Fin = 1 and a.&var. > b.Inicio)
             or (b.Flag_Ini = 0 and b.Flag_Fin = 0 and a.&var. > b.Inicio
                 and a.&var. <= b.Fin)
         );
    quit;
%mend _cal_apply_numeric_cuts;

%macro _cal_apply_categorical_buckets(data=, var=, out=);
    data &out.;
        set &data.;
        length Bucket_Label $200;
        Bucket_Label=strip(vvaluex("&var."));
        if missing(Bucket_Label) then Bucket_Label="00. Missing";
    run;
%mend _cal_apply_categorical_buckets;

%macro _cal_finalize_cat_buckets(data=, out=);
    proc sort data=&data.(keep=Bucket_Label) out=work._cal_cat_levels
        nodupkey;
        by Bucket_Label;
    run;

    data work._cal_cat_levels;
        set work._cal_cat_levels;
        Bucket_Order=_n_ - 1;
    run;

    proc sql;
        create table &out. as
        select a.*, b.Bucket_Order
        from &data. as a
        left join work._cal_cat_levels as b
          on a.Bucket_Label=b.Bucket_Label;
    quit;

    proc datasets library=work nolist nowarn;
        delete _cal_cat_levels;
    quit;
%mend _cal_finalize_cat_buckets;

%macro _cal_metric_detail(data=, variable=, var_seq=, var_type=, split=,
    calc_mode=STD, target=, score=, weight_var=, out=work._cal_detail_var);

    %local _cal_total;
    %let _cal_total=0;

    proc sql noprint;
        select count(*) into :_cal_total trimmed from &data.;
    quit;

    %if &_cal_total.=0 %then %do;
        data &out.;
            set work._cal_detail_all(obs=0);
        run;
        %return;
    %end;

    %if %upcase(&calc_mode.)=WGT %then %do;
        proc sql;
            create table work._cal_metric_base as
            select Bucket_Order,
                   Bucket_Label,
                   count(*) as N_Cuentas,
                   count(*) / &_cal_total. as Pct_Cuentas format=percent8.2,
                   sum(case when missing(&target.) then 0 else 1 end)
                       as Registros_RD,
                   sum(case when missing(&score.) then 0 else 1 end)
                       as Registros_PD,
                   sum(case when missing(&target.) or missing(&weight_var.)
                       then 0 else 1 end) as Registros_RD_Pond,
                   sum(case when missing(&score.) or missing(&weight_var.)
                       then 0 else 1 end) as Registros_PD_Pond,
                   case
                       when sum(case when missing(&target.) or
                           missing(&weight_var.) then 0 else &weight_var. end)
                           > 0
                       then sum(case when missing(&target.) or
                           missing(&weight_var.) then 0
                           else &target. * &weight_var. end)
                            / sum(case when missing(&target.) or
                           missing(&weight_var.) then 0 else &weight_var. end)
                       else .
                   end as RD format=percent8.2,
                   case
                       when sum(case when missing(&score.) or
                           missing(&weight_var.) then 0 else &weight_var. end)
                           > 0
                       then sum(case when missing(&score.) or
                           missing(&weight_var.) then 0
                           else &score. * &weight_var. end)
                            / sum(case when missing(&score.) or
                           missing(&weight_var.) then 0 else &weight_var. end)
                       else .
                   end as PD format=percent8.2
            from &data.
            group by Bucket_Order, Bucket_Label
            order by Bucket_Order, Bucket_Label;
        quit;
    %end;
    %else %do;
        proc sql;
            create table work._cal_metric_base as
            select Bucket_Order,
                   Bucket_Label,
                   count(*) as N_Cuentas,
                   count(*) / &_cal_total. as Pct_Cuentas format=percent8.2,
                   sum(case when missing(&target.) then 0 else 1 end)
                       as Registros_RD,
                   sum(case when missing(&score.) then 0 else 1 end)
                       as Registros_PD,
                   . as Registros_RD_Pond,
                   . as Registros_PD_Pond,
                   case
                       when calculated Registros_RD > 0
                       then sum(case when missing(&target.) then 0
                           else &target. end) / calculated Registros_RD
                       else .
                   end as RD format=percent8.2,
                   case
                       when calculated Registros_PD > 0
                       then sum(case when missing(&score.) then 0
                           else &score. end) / calculated Registros_PD
                       else .
                   end as PD format=percent8.2
            from &data.
            group by Bucket_Order, Bucket_Label
            order by Bucket_Order, Bucket_Label;
        quit;
    %end;

    data &out.;
        length Variable $64 Var_Type $3 Split $5 Calc_Mode $3;
        set work._cal_metric_base;
        Var_Seq=&var_seq.;
        Variable="&variable.";
        Var_Type="&var_type.";
        Split="&split.";
        Calc_Mode="%upcase(&calc_mode.)";
        format Pct_Cuentas RD PD LI_10 LS_10 LI_25 LS_25 percent8.2;
    run;

    %_cal_vasicek(datain=&out., est=PD, rho=0.005, alpha=0.10);
    %_cal_vasicek(datain=&out., est=PD, rho=0.005, alpha=0.25);

    proc datasets library=work nolist nowarn;
        delete _cal_metric_base;
    quit;

%mend _cal_metric_detail;

%macro _calibration_compute(train_data=, oot_data=, driver_meta=, target=,
    score_var=, weight_var=, groups=5, calc_weighted=0,
    out_detail=casuser._cal_detail, out_cuts=casuser._cal_cuts);

    %local _cal_nvars _i _var _type _seq _in_oot;
    %let _cal_nvars=0;

    data work._cal_train;
        set &train_data.;
    run;

    data work._cal_oot;
        set &oot_data.;
    run;

    %_cal_init_outputs(detail=work._cal_detail_all, cuts=work._cal_cuts_all);

    data _null_;
        set &driver_meta. end=eof;
        call symputx(cats("_cal_var", _n_), Variable, "L");
        call symputx(cats("_cal_type", _n_), Var_Type, "L");
        call symputx(cats("_cal_seq", _n_), Var_Seq, "L");
        call symputx(cats("_cal_oot", _n_), In_OOT, "L");
        if eof then call symputx("_cal_nvars", _n_, "L");
    run;

    %if %length(%superq(_cal_nvars))=0 %then %let _cal_nvars=0;

    %do _i=1 %to &_cal_nvars.;
        %let _var=&&_cal_var&_i.;
        %let _type=&&_cal_type&_i.;
        %let _seq=&&_cal_seq&_i.;
        %let _in_oot=&&_cal_oot&_i.;

        %put NOTE: [calibracion_compute] Procesando &_type. &_var.
            (Var_Seq=&_seq.).;

        %if %upcase(&_type.)=NUM %then %do;
            %_cal_prepare_numeric(data=work._cal_train, var=&_var.,
                out=work._cal_train_var);
            %_cal_build_numeric_cuts(data=work._cal_train_var, var=&_var.,
                variable=&_var., var_seq=&_seq., groups=&groups.,
                out=work._cal_var_cuts);
            %_cal_apply_numeric_cuts(data=work._cal_train_var, var=&_var.,
                cuts=work._cal_var_cuts, out=work._cal_train_tagged);

            proc append base=work._cal_cuts_all data=work._cal_var_cuts force;
            run;

            %_cal_metric_detail(data=work._cal_train_tagged, variable=&_var.,
                var_seq=&_seq., var_type=NUM, split=TRAIN, calc_mode=STD,
                target=&target., score=&score_var.,
                out=work._cal_detail_var);
            proc append base=work._cal_detail_all data=work._cal_detail_var
                force;
            run;

            %if &calc_weighted.=1 %then %do;
                %_cal_metric_detail(data=work._cal_train_tagged,
                    variable=&_var., var_seq=&_seq., var_type=NUM,
                    split=TRAIN, calc_mode=WGT, target=&target.,
                    score=&score_var., weight_var=&weight_var.,
                    out=work._cal_detail_var_w);
                proc append base=work._cal_detail_all
                    data=work._cal_detail_var_w force;
                run;
            %end;

            %if &_in_oot.=1 %then %do;
                %_cal_prepare_numeric(data=work._cal_oot, var=&_var.,
                    out=work._cal_oot_var);
                %_cal_apply_numeric_cuts(data=work._cal_oot_var, var=&_var.,
                    cuts=work._cal_var_cuts, out=work._cal_oot_tagged);

                %_cal_metric_detail(data=work._cal_oot_tagged,
                    variable=&_var., var_seq=&_seq., var_type=NUM,
                    split=OOT, calc_mode=STD, target=&target.,
                    score=&score_var., out=work._cal_detail_var);
                proc append base=work._cal_detail_all
                    data=work._cal_detail_var force;
                run;

                %if &calc_weighted.=1 %then %do;
                    %_cal_metric_detail(data=work._cal_oot_tagged,
                        variable=&_var., var_seq=&_seq., var_type=NUM,
                        split=OOT, calc_mode=WGT, target=&target.,
                        score=&score_var., weight_var=&weight_var.,
                        out=work._cal_detail_var_w);
                    proc append base=work._cal_detail_all
                        data=work._cal_detail_var_w force;
                    run;
                %end;
            %end;
        %end;
        %else %do;
            %_cal_apply_categorical_buckets(data=work._cal_train, var=&_var.,
                out=work._cal_train_cat0);
            %_cal_finalize_cat_buckets(data=work._cal_train_cat0,
                out=work._cal_train_tagged);

            %_cal_metric_detail(data=work._cal_train_tagged, variable=&_var.,
                var_seq=&_seq., var_type=CAT, split=TRAIN, calc_mode=STD,
                target=&target., score=&score_var.,
                out=work._cal_detail_var);
            proc append base=work._cal_detail_all data=work._cal_detail_var
                force;
            run;

            %if &calc_weighted.=1 %then %do;
                %_cal_metric_detail(data=work._cal_train_tagged,
                    variable=&_var., var_seq=&_seq., var_type=CAT,
                    split=TRAIN, calc_mode=WGT, target=&target.,
                    score=&score_var., weight_var=&weight_var.,
                    out=work._cal_detail_var_w);
                proc append base=work._cal_detail_all
                    data=work._cal_detail_var_w force;
                run;
            %end;

            %if &_in_oot.=1 %then %do;
                %_cal_apply_categorical_buckets(data=work._cal_oot,
                    var=&_var., out=work._cal_oot_cat0);
                %_cal_finalize_cat_buckets(data=work._cal_oot_cat0,
                    out=work._cal_oot_tagged);

                %_cal_metric_detail(data=work._cal_oot_tagged,
                    variable=&_var., var_seq=&_seq., var_type=CAT,
                    split=OOT, calc_mode=STD, target=&target.,
                    score=&score_var., out=work._cal_detail_var);
                proc append base=work._cal_detail_all
                    data=work._cal_detail_var force;
                run;

                %if &calc_weighted.=1 %then %do;
                    %_cal_metric_detail(data=work._cal_oot_tagged,
                        variable=&_var., var_seq=&_seq., var_type=CAT,
                        split=OOT, calc_mode=WGT, target=&target.,
                        score=&score_var., weight_var=&weight_var.,
                        out=work._cal_detail_var_w);
                    proc append base=work._cal_detail_all
                        data=work._cal_detail_var_w force;
                    run;
                %end;
            %end;
        %end;

        proc datasets library=work nolist nowarn;
            delete _cal_train_var _cal_oot_var _cal_var_cuts _cal_train_tagged
                _cal_oot_tagged _cal_train_cat0 _cal_oot_cat0
                _cal_detail_var _cal_detail_var_w;
        quit;
    %end;

    proc sort data=work._cal_detail_all;
        by Var_Seq Split Calc_Mode Bucket_Order;
    run;

    proc sort data=work._cal_cuts_all;
        by Var_Seq Bucket_Order;
    run;

    data &out_detail.;
        set work._cal_detail_all;
    run;

    data &out_cuts.;
        set work._cal_cuts_all;
    run;

%mend _calibration_compute;
