/* =========================================================================
gini_model_compute.sas - Gini del modelo (general y mensual)
========================================================================= */

%macro _gini_model_general_split(data=, split=, target=, score=,
    with_missing=1, model_low=, model_high=, out=casuser._gini_model_split);

    %local _gini_n _smdcr _gini_ft_exists;

    proc sql noprint;
        create table &out. as
        select "&split." as Split length=5,
            count(*) as N_Total,
            sum(&target.) as N_Default,
            sum(1-&target.) as N_No_Default,
            calculated N_Default / calculated N_Total as Tasa_Default
                format=percent8.2
        from &data.;
    quit;

    %_gini_count_rows(data=&data., target=&target., score=&score.,
        with_missing=&with_missing., outvar=_gini_n);
    %let _smdcr=.;

    %_gini_freqtab_general(data=&data., target=&target., score=&score.,
        with_missing=&with_missing., out=casuser._gini_model_ft);

    %let _gini_ft_exists=0;
    proc sql noprint;
        select count(*) into :_gini_ft_exists trimmed
        from dictionary.tables
        where upcase(libname)='CASUSER'
          and upcase(memname)='_GINI_MODEL_FT';
    quit;

    %if &_gini_ft_exists. > 0 %then %do;
        proc sql noprint;
            select max(_SMDCR_) into :_smdcr trimmed
            from casuser._gini_model_ft;
        quit;
    %end;

    data &out.;
        set &out.;
        length Evaluacion $15;
        N_Gini=input(symget('_gini_n'), best32.);
        Smdcr_Raw=input(symget('_smdcr'), best32.);
        Gini=abs(Smdcr_Raw);
        if N_Gini > 0 and not missing(Gini) then do;
            SE=sqrt(Gini * (1 - Gini) / N_Gini);
            IC_95_Lower=max(0, Gini - 1.96 * SE);
            IC_95_Upper=min(1, Gini + 1.96 * SE);
        end;
        if missing(Gini) then Evaluacion="SIN DATOS";
        else if Gini >= &model_high. then Evaluacion="SATISFACTORIO";
        else if Gini >= &model_low. then Evaluacion="ACEPTABLE";
        else Evaluacion="BAJO";
        format Gini Smdcr_Raw SE IC_95_Lower IC_95_Upper 8.4;
    run;

    proc datasets library=casuser nolist nowarn;
        delete _gini_model_ft;
    quit;

%mend _gini_model_general_split;

%macro _gini_model_general(data=, split_var=Split, target=, score=,
    with_missing=1, model_low=, model_high=, out=casuser._gini_model_general);

    %_gini_model_general_split(
        data=&data.(where=(upcase(strip(&split_var.))='TRAIN')),
        split=TRAIN,
        target=&target.,
        score=&score.,
        with_missing=&with_missing.,
        model_low=&model_low.,
        model_high=&model_high.,
        out=casuser._gini_model_train
    );

    %_gini_model_general_split(
        data=&data.(where=(upcase(strip(&split_var.))='OOT')),
        split=OOT,
        target=&target.,
        score=&score.,
        with_missing=&with_missing.,
        model_low=&model_low.,
        model_high=&model_high.,
        out=casuser._gini_model_oot
    );

    data &out.;
        set casuser._gini_model_train casuser._gini_model_oot;
    run;

    %local _gini_train;
    %let _gini_train=.;
    proc sql noprint;
        select Gini into :_gini_train trimmed from &out. where Split="TRAIN";
    quit;

    data &out.;
        set &out.;
        if Split="OOT" and not missing(Gini) and input(symget('_gini_train'),
            best32.) > 0 then Degradacion=
            (input(symget('_gini_train'), best32.) - Gini) /
            input(symget('_gini_train'), best32.);
        format Degradacion percent8.2;
    run;

    proc datasets library=casuser nolist nowarn;
        delete _gini_model_train _gini_model_oot;
    quit;

%mend _gini_model_general;

%macro _gini_model_monthly_split(data=, split=, target=, score=, byvar=,
    with_missing=1, model_low=, model_high=,
    out=casuser._gini_model_month_split);

    proc fedsql sessref=conn;
        create table casuser._gini_model_src {options replace=true} as
        select *
        from &data.;
    quit;

    %_gini_sort_cas(table_name=_gini_model_src,
        orderby=%str({"&byvar."}));

    proc sql noprint;
        create table casuser._gini_model_cnt as
        select "&split." as Split length=5,
            &byvar. as Periodo,
            count(*) as N_Total,
            sum(&target.) as N_Default,
            sum(1-&target.) as N_No_Default,
            calculated N_Default / calculated N_Total as Tasa_Default
                format=percent8.2
        from casuser._gini_model_src
        group by &byvar.;
    quit;

    %if &with_missing.=1 %then %do;
        proc sql noprint;
            create table casuser._gini_model_ng as
            select &byvar. as Periodo,
                count(*) as N_Gini
            from casuser._gini_model_src
            where not missing(&target.)
            group by &byvar.;
        quit;

        proc freqtab data=casuser._gini_model_src noprint missing;
            by &byvar.;
            tables &target. * &score. / measures;
            output out=casuser._gini_model_ftb smdcr;
        run;
    %end;
    %else %do;
        proc sql noprint;
            create table casuser._gini_model_ng as
            select &byvar. as Periodo,
                count(*) as N_Gini
            from casuser._gini_model_src
            where not missing(&target.) and not missing(&score.)
            group by &byvar.;
        quit;

        proc freqtab data=casuser._gini_model_src noprint;
            by &byvar.;
            tables &target. * &score. / measures;
            output out=casuser._gini_model_ftb smdcr;
        run;
    %end;

    proc sql noprint;
        create table &out. as
        select c.Split,
            c.Periodo,
            c.N_Total,
            c.N_Default,
            c.N_No_Default,
            c.Tasa_Default,
            n.N_Gini,
            f._SMDCR_ as Smdcr_Raw format=8.4,
            abs(f._SMDCR_) as Gini format=8.4
        from casuser._gini_model_cnt c
        left join casuser._gini_model_ng n
            on c.Periodo=n.Periodo
        left join casuser._gini_model_ftb f
            on c.Periodo=f.&byvar.;
    quit;

    data &out.;
        set &out.;
        length Evaluacion $15;
        if missing(Gini) then Evaluacion="SIN DATOS";
        else if Gini >= &model_high. then Evaluacion="SATISFACTORIO";
        else if Gini >= &model_low. then Evaluacion="ACEPTABLE";
        else Evaluacion="BAJO";
    run;

    proc datasets library=casuser nolist nowarn;
        delete _gini_model_src _gini_model_cnt _gini_model_ng _gini_model_ftb;
    quit;

%mend _gini_model_monthly_split;

%macro _gini_model_monthly(data=, split_var=Split, target=, score=, byvar=,
    with_missing=1, model_low=, model_high=, trend_delta=0.03,
    out=casuser._gini_model_monthly);

    %_gini_model_monthly_split(
        data=&data.(where=(upcase(strip(&split_var.))='TRAIN')),
        split=TRAIN,
        target=&target.,
        score=&score.,
        byvar=&byvar.,
        with_missing=&with_missing.,
        model_low=&model_low.,
        model_high=&model_high.,
        out=casuser._gini_model_train_m
    );

    %_gini_model_monthly_split(
        data=&data.(where=(upcase(strip(&split_var.))='OOT')),
        split=OOT,
        target=&target.,
        score=&score.,
        byvar=&byvar.,
        with_missing=&with_missing.,
        model_low=&model_low.,
        model_high=&model_high.,
        out=casuser._gini_model_oot_m
    );

    data casuser._gini_model_monthly_all;
        set casuser._gini_model_train_m casuser._gini_model_oot_m;
    run;

    proc sql noprint;
        create table casuser._gini_model_fl as
        select Split, min(Periodo) as First_Period, max(Periodo) as Last_Period
        from casuser._gini_model_monthly_all
        group by Split;

        create table casuser._gini_model_trend as
        select a.Split,
            f.Gini as Gini_First format=8.4,
            l.Gini as Gini_Last format=8.4,
            (l.Gini - f.Gini) as Delta_Gini format=8.4
        from casuser._gini_model_fl a
        left join casuser._gini_model_monthly_all f
            on a.Split=f.Split and a.First_Period=f.Periodo
        left join casuser._gini_model_monthly_all l
            on a.Split=l.Split and a.Last_Period=l.Periodo;

        create table casuser._gini_model_monthly_out as
        select a.*,
            t.Gini_First,
            t.Gini_Last,
            t.Delta_Gini,
            case
                when t.Gini_First is missing or t.Gini_Last is missing
                    then "SIN DATOS"
                when t.Delta_Gini < -&trend_delta. then "EMPEORANDO"
                when t.Delta_Gini > &trend_delta. then "MEJORANDO"
                else "ESTABLE"
            end as Tendencia length=15
        from casuser._gini_model_monthly_all a
        left join casuser._gini_model_trend t
            on a.Split=t.Split;
    quit;

    data &out.;
        set casuser._gini_model_monthly_out;
    run;

    proc datasets library=casuser nolist nowarn;
        delete _gini_model_train_m _gini_model_oot_m _gini_model_monthly_all
            _gini_model_fl _gini_model_trend _gini_model_monthly_out;
    quit;

%mend _gini_model_monthly;
