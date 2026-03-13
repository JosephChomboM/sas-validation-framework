/* =========================================================================
gini_variable_compute.sas - Gini de variables (general, comparativo y mensual)
========================================================================= */

%macro _gini_variables_general_split(data=, split=, target=, vars_num=,
    with_missing=1, min_n_valid=30, var_low=, var_high=,
    out=work._gini_vars_split);

    data &out.;
        length Variable $64 Split $5 N_Total N_Valid N_Default N_Gini Ranking 8
            Pct_Valid Gini Smdcr_Raw 8 Evaluacion $15;
        format Pct_Valid percent8.2 Gini Smdcr_Raw 8.4;
        stop;
    run;

    %if %length(%superq(vars_num))=0 %then %return;

    %local _i _var _n_total _n_valid _n_default _n_gini _smdcr;
    %let _i=1;
    %let _var=%scan(&vars_num., &_i., %str( ));

    %do %while(%length(&_var.) > 0);
        proc sql noprint;
            select count(*),
                sum(case when not missing(&_var.) then 1 else 0 end),
                sum(&target.)
            into :_n_total trimmed, :_n_valid trimmed, :_n_default trimmed
            from &data.;
        quit;

        %_gini_count_rows(data=&data., target=&target., score=&_var.,
            with_missing=&with_missing., outvar=_n_gini);

        %let _smdcr=.;
        %if %sysfunc(inputn(&_n_gini., best32.)) >= &min_n_valid. %then %do;
            %_gini_freqtab_general(data=&data., target=&target.,
                score=&_var., with_missing=&with_missing.,
                out=work._gini_var_ft);
            %if %sysfunc(exist(work._gini_var_ft)) %then %do;
                proc sql noprint;
                    select max(_SMDCR_) into :_smdcr trimmed
                    from work._gini_var_ft;
                quit;
            %end;
        %end;

        data work._gini_var_row;
            length Variable $64 Split $5 Evaluacion $15;
            Variable="%upcase(&_var.)";
            Split="&split.";
            N_Total=input(symget('_n_total'), best32.);
            N_Valid=input(symget('_n_valid'), best32.);
            N_Default=input(symget('_n_default'), best32.);
            N_Gini=input(symget('_n_gini'), best32.);
            if N_Total > 0 then Pct_Valid=N_Valid / N_Total;
            Smdcr_Raw=input(symget('_smdcr'), best32.);
            Gini=abs(Smdcr_Raw);
            if N_Gini < &min_n_valid. then Evaluacion="MIN DATOS";
            else if missing(Gini) then Evaluacion="SIN DATOS";
            else if Gini >= &var_high. then Evaluacion="SATISFACTORIO";
            else if Gini >= &var_low. then Evaluacion="ACEPTABLE";
            else Evaluacion="BAJO";
            format Pct_Valid percent8.2 Gini Smdcr_Raw 8.4;
        run;

        proc append base=&out. data=work._gini_var_row force;
        run;

        proc datasets library=work nolist nowarn;
            delete _gini_var_ft _gini_var_row;
        quit;

        %let _i=%eval(&_i. + 1);
        %let _var=%scan(&vars_num., &_i., %str( ));
    %end;

    data work._gini_vars_ranked;
        set &out.;
        SortMissing=missing(Gini);
    run;

    proc sort data=work._gini_vars_ranked;
        by SortMissing descending Gini Variable;
    run;

    data &out.;
        set work._gini_vars_ranked;
        retain _rank 0;
        if _n_=1 then _rank=0;
        if SortMissing=0 then do;
            _rank + 1;
            Ranking=_rank;
        end;
        else Ranking=.;
        drop _rank SortMissing;
    run;

    proc datasets library=work nolist nowarn;
        delete _gini_vars_ranked;
    quit;

%mend _gini_variables_general_split;

%macro _gini_variables_general(train_data=, oot_data=, target=,
    vars_num_train=, vars_num_oot=, with_missing=1, min_n_valid=30, var_low=,
    var_high=,
    out=casuser._gini_vars_general);

    %if %length(%superq(vars_num_train))=0 and %length(%superq(vars_num_oot))=0
        %then %do;
        data &out.;
            length Variable $64 Split $5 N_Total N_Valid N_Default N_Gini Ranking 8
                Pct_Valid Gini Smdcr_Raw 8 Evaluacion $15;
            format Pct_Valid percent8.2 Gini Smdcr_Raw 8.4;
            stop;
        run;
        %return;
    %end;

    %_gini_variables_general_split(data=&train_data., split=TRAIN,
        target=&target., vars_num=&vars_num_train.,
        with_missing=&with_missing.,
        min_n_valid=&min_n_valid., var_low=&var_low., var_high=&var_high.,
        out=work._gini_vars_train);

    %_gini_variables_general_split(data=&oot_data., split=OOT,
        target=&target., vars_num=&vars_num_oot., with_missing=&with_missing.,
        min_n_valid=&min_n_valid., var_low=&var_low., var_high=&var_high.,
        out=work._gini_vars_oot);

    data &out.;
        set work._gini_vars_train work._gini_vars_oot;
    run;

    proc datasets library=work nolist nowarn;
        delete _gini_vars_train _gini_vars_oot;
    quit;

%mend _gini_variables_general;

%macro _gini_variables_compare(data=, delta_warn=0.05,
    out=casuser._gini_vars_compare);

    proc sql noprint;
        create table &out. as
        select coalesce(t.Variable, o.Variable) as Variable length=64,
            t.Gini as Gini_Train format=8.4,
            t.Ranking as Rank_Train,
            o.Gini as Gini_OOT format=8.4,
            o.Ranking as Rank_OOT,
            (t.Gini - o.Gini) as Delta_Gini format=8.4,
            abs(t.Ranking - o.Ranking) as Delta_Rank,
            case
                when t.Gini is missing or o.Gini is missing then "SIN DATOS"
                when calculated Delta_Gini > &delta_warn. then "DEGRADACION"
                when calculated Delta_Gini < -&delta_warn. then "MEJORA"
                else "ESTABLE"
            end as Estabilidad length=15
        from (select * from &data. where Split="TRAIN") t
        full join (select * from &data. where Split="OOT") o
            on t.Variable=o.Variable
        order by Rank_Train, Variable;
    quit;

%mend _gini_variables_compare;

%macro _gini_variables_monthly_split(data=, split=, target=, vars_num=,
    byvar=, with_missing=1, min_n_valid=30, var_low=, var_high=,
    out=work._gini_vars_month_split);

    data &out.;
        length Variable $64 Split $5 Periodo 8 N_Total N_Valid N_Default N_Gini 8
            Gini Smdcr_Raw 8 Evaluacion $15;
        format Gini Smdcr_Raw 8.4;
        stop;
    run;

    %if %length(%superq(vars_num))=0 %then %return;

    %local _i _var _n_periods _j _period _n_gini _smdcr;
    %let _i=1;
    %let _var=%scan(&vars_num., &_i., %str( ));

    %do %while(%length(&_var.) > 0);
        proc sql noprint;
            select distinct &byvar. into :_gini_vprd1- from &data.
                order by &byvar.;
            %let _n_periods=&sqlobs.;
        quit;

        %do _j=1 %to &_n_periods.;
            %let _period=&&_gini_vprd&_j.;
            %_gini_count_rows(data=&data.(where=(&byvar.=&_period.)),
                target=&target., score=&_var., with_missing=&with_missing.,
                outvar=_n_gini);
            %let _smdcr=.;

            %if %sysfunc(inputn(&_n_gini., best32.)) >= &min_n_valid. %then %do;
                %_gini_freqtab_general(data=&data.(where=(&byvar.=&_period.)),
                    target=&target., score=&_var.,
                    with_missing=&with_missing., out=work._gini_var_ftb);
                %if %sysfunc(exist(work._gini_var_ftb)) %then %do;
                    proc sql noprint;
                        select max(_SMDCR_) into :_smdcr trimmed from
                            work._gini_var_ftb;
                    quit;
                %end;
            %end;

            proc sql noprint;
                create table work._gini_var_row as
                select "%upcase(&_var.)" as Variable length=64,
                    "&split." as Split length=5,
                    &_period. as Periodo,
                    count(*) as N_Total,
                    sum(case when not missing(&_var.) then 1 else 0 end)
                        as N_Valid,
                    sum(&target.) as N_Default
                from &data.
                where &byvar.=&_period.;
            quit;

            data work._gini_var_row;
                set work._gini_var_row;
                length Evaluacion $15;
                N_Gini=input(symget('_n_gini'), best32.);
                Smdcr_Raw=input(symget('_smdcr'), best32.);
                Gini=abs(Smdcr_Raw);
                if N_Gini < &min_n_valid. then do;
                    Smdcr_Raw=.;
                    Gini=.;
                    Evaluacion="MIN DATOS";
                end;
                else if missing(Gini) then Evaluacion="SIN DATOS";
                else if Gini >= &var_high. then Evaluacion="SATISFACTORIO";
                else if Gini >= &var_low. then Evaluacion="ACEPTABLE";
                else Evaluacion="BAJO";
                format Gini Smdcr_Raw 8.4;
            run;

            proc append base=&out. data=work._gini_var_row force;
            run;

            proc datasets library=work nolist nowarn;
                delete _gini_var_ftb _gini_var_row;
            quit;
        %end;

        %let _i=%eval(&_i. + 1);
        %let _var=%scan(&vars_num., &_i., %str( ));
    %end;

    proc sort data=&out.;
        by Split Variable Periodo;
    run;

%mend _gini_variables_monthly_split;

%macro _gini_variables_monthly(train_data=, oot_data=, target=,
    vars_num_train=, vars_num_oot=, byvar=, with_missing=1, min_n_valid=30,
    var_low=, var_high=,
    out=casuser._gini_vars_detail);

    %if %length(%superq(vars_num_train))=0 and %length(%superq(vars_num_oot))=0
        %then %do;
        data &out.;
            length Variable $64 Split $5 Periodo 8 N_Total N_Valid N_Default N_Gini 8
                Gini Smdcr_Raw 8 Evaluacion $15;
            format Gini Smdcr_Raw 8.4;
            stop;
        run;
        %return;
    %end;

    %_gini_variables_monthly_split(data=&train_data., split=TRAIN,
        target=&target., vars_num=&vars_num_train., byvar=&byvar.,
        with_missing=&with_missing., min_n_valid=&min_n_valid.,
        var_low=&var_low., var_high=&var_high., out=work._gini_vars_trn_m);

    %_gini_variables_monthly_split(data=&oot_data., split=OOT,
        target=&target., vars_num=&vars_num_oot., byvar=&byvar.,
        with_missing=&with_missing., min_n_valid=&min_n_valid.,
        var_low=&var_low., var_high=&var_high., out=work._gini_vars_oot_m);

    data &out.;
        set work._gini_vars_trn_m work._gini_vars_oot_m;
    run;

    proc datasets library=work nolist nowarn;
        delete _gini_vars_trn_m _gini_vars_oot_m;
    quit;

%mend _gini_variables_monthly;

%macro _gini_variables_summary(data=, var_low=, var_high=, trend_delta=0.03,
    out=casuser._gini_vars_summary);

    proc sql noprint;
        create table work._gini_vars_base as
        select Variable,
            Split,
            count(*) as N_Periodos,
            min(Periodo) as First_Period,
            max(Periodo) as Last_Period,
            mean(Gini) as Gini_Promedio format=8.4,
            min(Gini) as Gini_Min format=8.4,
            max(Gini) as Gini_Max format=8.4,
            std(Gini) as Gini_Std format=8.4
        from &data.
        where Gini is not missing
        group by Variable, Split;

        create table &out. as
        select a.Variable,
            a.Split,
            a.N_Periodos,
            a.First_Period,
            a.Last_Period,
            f.Gini as Gini_First format=8.4,
            l.Gini as Gini_Last format=8.4,
            a.Gini_Promedio,
            a.Gini_Min,
            a.Gini_Max,
            a.Gini_Std,
            (l.Gini - f.Gini) as Delta_Gini format=8.4,
            case
                when f.Gini is missing or l.Gini is missing then "SIN DATOS"
                when calculated Delta_Gini < -&trend_delta. then "EMPEORANDO"
                when calculated Delta_Gini > &trend_delta. then "MEJORANDO"
                else "ESTABLE"
            end as Tendencia length=15,
            case
                when a.Gini_Promedio is missing then "SIN DATOS"
                when a.Gini_Promedio >= &var_high. then "SATISFACTORIO"
                when a.Gini_Promedio >= &var_low. then "ACEPTABLE"
                else "BAJO"
            end as Evaluacion length=15
        from work._gini_vars_base a
        left join &data. f
            on a.Variable=f.Variable and a.Split=f.Split and
            a.First_Period=f.Periodo
        left join &data. l
            on a.Variable=l.Variable and a.Split=l.Split and
            a.Last_Period=l.Periodo
        order by a.Split, a.Gini_Promedio desc, a.Variable;
    quit;

    proc datasets library=work nolist nowarn;
        delete _gini_vars_base;
    quit;

%mend _gini_variables_summary;
