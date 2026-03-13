/* =========================================================================
gini_model_compute.sas - Gini del modelo (general y mensual)
========================================================================= */

%macro _gini_model_general_split(data=, split=, target=, score=,
    with_missing=1, model_low=, model_high=, out=work._gini_model_split);

    %local _gini_n _smdcr;

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
        with_missing=&with_missing., out=work._gini_model_ft);

    %if %sysfunc(exist(work._gini_model_ft)) %then %do;
        proc sql noprint;
            select max(_SMDCR_) into :_smdcr trimmed from work._gini_model_ft;
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

    proc datasets library=work nolist nowarn;
        delete _gini_model_ft;
    quit;

%mend _gini_model_general_split;

%macro _gini_model_general(train_data=, oot_data=, target=, score=,
    with_missing=1, model_low=, model_high=, out=casuser._gini_model_general);

    %_gini_model_general_split(data=&train_data., split=TRAIN, target=&target.,
        score=&score., with_missing=&with_missing., model_low=&model_low.,
        model_high=&model_high., out=work._gini_model_train);

    %_gini_model_general_split(data=&oot_data., split=OOT, target=&target.,
        score=&score., with_missing=&with_missing., model_low=&model_low.,
        model_high=&model_high., out=work._gini_model_oot);

    data &out.;
        set work._gini_model_train work._gini_model_oot;
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

    proc datasets library=work nolist nowarn;
        delete _gini_model_train _gini_model_oot;
    quit;

%mend _gini_model_general;

%macro _gini_model_monthly_split(data=, split=, target=, score=, byvar=,
    with_missing=1, model_low=, model_high=, out=work._gini_model_month_split);

    data &out.;
        length Split $5 Periodo 8 N_Total N_Default N_No_Default N_Gini 8
            Tasa_Default Gini Smdcr_Raw 8 Evaluacion $15;
        format Tasa_Default percent8.2 Gini Smdcr_Raw 8.4;
        stop;
    run;

    %local _n_periods _i _period _n_gini _smdcr;
    proc sql noprint;
        select distinct &byvar. into :_gini_prd1- from &data. order by &byvar.;
        %let _n_periods=&sqlobs.;
    quit;

    %do _i=1 %to &_n_periods.;
        %let _period=&&_gini_prd&_i.;
        %_gini_count_rows(data=&data.(where=(&byvar.=&_period.)),
            target=&target., score=&score., with_missing=&with_missing.,
            outvar=_n_gini);
        %let _smdcr=.;

        %if %sysfunc(inputn(&_n_gini., best32.)) > 0 %then %do;
            %_gini_freqtab_general(data=&data.(where=(&byvar.=&_period.)),
                target=&target., score=&score., with_missing=&with_missing.,
                out=work._gini_model_ftb);
            %if %sysfunc(exist(work._gini_model_ftb)) %then %do;
                proc sql noprint;
                    select max(_SMDCR_) into :_smdcr trimmed from
                        work._gini_model_ftb;
                quit;
            %end;
        %end;

        proc sql noprint;
            create table work._gini_model_row as
            select "&split." as Split length=5,
                &_period. as Periodo,
                count(*) as N_Total,
                sum(&target.) as N_Default,
                sum(1-&target.) as N_No_Default,
                calculated N_Default / calculated N_Total as Tasa_Default
                    format=percent8.2
            from &data.
            where &byvar.=&_period.;
        quit;

        data work._gini_model_row;
            set work._gini_model_row;
            length Evaluacion $15;
            N_Gini=input(symget('_n_gini'), best32.);
            Smdcr_Raw=input(symget('_smdcr'), best32.);
            Gini=abs(Smdcr_Raw);
            if missing(Gini) then Evaluacion="SIN DATOS";
            else if Gini >= &model_high. then Evaluacion="SATISFACTORIO";
            else if Gini >= &model_low. then Evaluacion="ACEPTABLE";
            else Evaluacion="BAJO";
            format Gini Smdcr_Raw 8.4;
        run;

        proc append base=&out. data=work._gini_model_row force;
        run;

        proc datasets library=work nolist nowarn;
            delete _gini_model_ftb _gini_model_row;
        quit;
    %end;

    proc sort data=&out.;
        by Periodo;
    run;

%mend _gini_model_monthly_split;

%macro _gini_model_monthly(train_data=, oot_data=, target=, score=, byvar=,
    with_missing=1, model_low=, model_high=, trend_delta=0.03,
    out=casuser._gini_model_monthly);

    %_gini_model_monthly_split(data=&train_data., split=TRAIN, target=&target.,
        score=&score., byvar=&byvar., with_missing=&with_missing.,
        model_low=&model_low., model_high=&model_high.,
        out=work._gini_model_train_m);

    %_gini_model_monthly_split(data=&oot_data., split=OOT, target=&target.,
        score=&score., byvar=&byvar., with_missing=&with_missing.,
        model_low=&model_low., model_high=&model_high.,
        out=work._gini_model_oot_m);

    data work._gini_model_monthly_all;
        set work._gini_model_train_m work._gini_model_oot_m;
    run;

    proc sql noprint;
        create table work._gini_model_fl as
        select Split, min(Periodo) as First_Period, max(Periodo) as Last_Period
        from work._gini_model_monthly_all
        group by Split;

        create table work._gini_model_trend as
        select a.Split,
            f.Gini as Gini_First format=8.4,
            l.Gini as Gini_Last format=8.4,
            (l.Gini - f.Gini) as Delta_Gini format=8.4
        from work._gini_model_fl a
        left join work._gini_model_monthly_all f
            on a.Split=f.Split and a.First_Period=f.Periodo
        left join work._gini_model_monthly_all l
            on a.Split=l.Split and a.Last_Period=l.Periodo;
    quit;

    proc sql noprint;
        create table &out. as
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
        from work._gini_model_monthly_all a
        left join work._gini_model_trend t
            on a.Split=t.Split
        order by a.Split, a.Periodo;
    quit;

    proc datasets library=work nolist nowarn;
        delete _gini_model_train_m _gini_model_oot_m _gini_model_monthly_all
            _gini_model_fl _gini_model_trend;
    quit;

%mend _gini_model_monthly;
