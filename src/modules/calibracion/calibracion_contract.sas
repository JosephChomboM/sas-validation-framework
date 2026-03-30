/* =========================================================================
calibracion_contract.sas - Validaciones pre-ejecucion del modulo Calibracion
========================================================================= */
%macro calibracion_contract(input_caslib=, input_table=, split_var=Split,
    vars_num=, vars_cat=, target=, score_var=, byvar=, def_cld=, monto_var=);

    %let _cal_rc=0;

    %local _cal_has_table _cal_nobs_total _cal_has_col _cal_nobs_trn
        _cal_nobs_oot _cal_nobs_trn_filt _cal_nobs_oot_filt;

    %if %length(%superq(vars_num))=0 and %length(%superq(vars_cat))=0 %then %do;
        %put ERROR: [calibracion_contract] No se proporcionaron drivers
            numericos ni categoricos.;
        %let _cal_rc=1;
        %return;
    %end;

    %if %length(%superq(target))=0 %then %do;
        %put ERROR: [calibracion_contract] target no definido.;
        %let _cal_rc=1;
        %return;
    %end;

    %if %length(%superq(score_var))=0 %then %do;
        %put ERROR: [calibracion_contract] score_var no definido.;
        %let _cal_rc=1;
        %return;
    %end;

    %if %length(%superq(byvar))=0 %then %do;
        %put ERROR: [calibracion_contract] byvar no definido.;
        %let _cal_rc=1;
        %return;
    %end;

    %if %length(%superq(def_cld))=0 %then %do;
        %put ERROR: [calibracion_contract] def_cld no definido.;
        %let _cal_rc=1;
        %return;
    %end;

    proc sql noprint;
        select count(*) into :_cal_has_table trimmed
        from dictionary.tables
        where upcase(libname)=upcase("&input_caslib.")
          and upcase(memname)=upcase("&input_table.");
    quit;

    %if &_cal_has_table.=0 %then %do;
        %put ERROR: [calibracion_contract] Input &input_caslib..&input_table.
            no accesible.;
        %let _cal_rc=1;
        %return;
    %end;

    proc sql noprint;
        select count(*) into :_cal_nobs_total trimmed
        from &input_caslib..&input_table.;
    quit;

    %if &_cal_nobs_total.=0 %then %do;
        %put ERROR: [calibracion_contract] Input unificado vacio.;
        %let _cal_rc=1;
        %return;
    %end;

    %macro _cal_chk(mem=, col=, req=1);
        %let _cal_has_col=0;
        proc sql noprint;
            select count(*) into :_cal_has_col trimmed
            from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&mem.")
              and upcase(name)=upcase("&col.");
        quit;
        %if &_cal_has_col.=0 %then %do;
            %if &req.=1 %then %do;
                %put ERROR: [calibracion_contract] &col. no encontrada en
                    &mem..;
                %let _cal_rc=1;
            %end;
            %else %do;
                %put WARNING: [calibracion_contract] &col. no encontrada en
                    &mem..;
            %end;
        %end;
    %mend;

    %_cal_chk(mem=&input_table., col=&target., req=1);
    %if &_cal_rc.=1 %then %return;
    %_cal_chk(mem=&input_table., col=&score_var., req=1);
    %if &_cal_rc.=1 %then %return;
    %_cal_chk(mem=&input_table., col=&byvar., req=1);
    %if &_cal_rc.=1 %then %return;
    %_cal_chk(mem=&input_table., col=&split_var., req=1);
    %if &_cal_rc.=1 %then %return;

    %if %length(%superq(monto_var)) > 0 %then %do;
        %_cal_chk(mem=&input_table., col=&monto_var., req=0);
    %end;

    proc sql noprint;
        select count(*) into :_cal_nobs_trn_filt trimmed
        from &input_caslib..&input_table.
        where upcase(&split_var.)='TRAIN'
          and &byvar. <= &def_cld.;
        select count(*) into :_cal_nobs_oot_filt trimmed
        from &input_caslib..&input_table.
        where upcase(&split_var.)='OOT'
          and &byvar. <= &def_cld.;
        select count(*) into :_cal_nobs_trn trimmed
        from &input_caslib..&input_table.
        where upcase(&split_var.)='TRAIN';
        select count(*) into :_cal_nobs_oot trimmed
        from &input_caslib..&input_table.
        where upcase(&split_var.)='OOT';
    quit;

    %if &_cal_nobs_trn.=0 %then %do;
        %put ERROR: [calibracion_contract] Split TRAIN ausente en input.;
        %let _cal_rc=1;
        %return;
    %end;

    %if &_cal_nobs_oot.=0 %then %do;
        %put ERROR: [calibracion_contract] Split OOT ausente en input.;
        %let _cal_rc=1;
        %return;
    %end;

    %if &_cal_nobs_trn_filt.=0 %then %do;
        %put ERROR: [calibracion_contract] TRAIN no tiene observaciones
            validas con &byvar. <= &def_cld..;
        %let _cal_rc=1;
        %return;
    %end;

    %if &_cal_nobs_oot_filt.=0 %then %do;
        %put ERROR: [calibracion_contract] OOT no tiene observaciones validas
            con &byvar. <= &def_cld..;
        %let _cal_rc=1;
        %return;
    %end;

    %put NOTE: [calibracion_contract] OK - input total=&_cal_nobs_total. obs;
    %put NOTE: [calibracion_contract] TRAIN=&_cal_nobs_trn. obs,
        OOT=&_cal_nobs_oot. obs.;
    %put NOTE: [calibracion_contract] TRAIN filtrado=&_cal_nobs_trn_filt.
        OOT filtrado=&_cal_nobs_oot_filt..;
    %put NOTE: [calibracion_contract] target=&target. score=&score_var.
        byvar=&byvar. split=&split_var. def_cld=&def_cld.
        monto=&monto_var..;

%mend calibracion_contract;
