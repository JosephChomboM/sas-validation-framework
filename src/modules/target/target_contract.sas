/* =========================================================================
target_contract.sas - Validaciones pre-ejecucion del modulo Target

Verifica:
1) target, byvar y def_cld definidos
2) TRAIN y OOT accesibles y con observaciones
3) byvar y target presentes en ambas tablas
4) TRAIN y OOT con observaciones dentro de la ventana byvar <= def_cld
5) monto opcional presente en ambas tablas para habilitar analisis ponderados

Setea:
- &_tgt_rc = 0|1
- &_tgt_has_monto = 0|1
========================================================================= */

%macro _target_var_exists(data=, var=, outvar=_tgt_exists);
    %local _dsid _rc _exists;
    %let _exists=0;
    %let _dsid=%sysfunc(open(&data.));

    %if &_dsid. > 0 %then %do;
        %if %sysfunc(varnum(&_dsid., &var.)) > 0 %then %let _exists=1;
        %let _rc=%sysfunc(close(&_dsid.));
    %end;

    %let &outvar=&_exists.;
%mend _target_var_exists;

%macro target_contract(input_caslib=, train_table=, oot_table=, target=,
    byvar=, monto_var=, def_cld=);

    %global _tgt_rc _tgt_has_monto;
    %let _tgt_rc=0;
    %let _tgt_has_monto=0;

    %local _tgt_nobs_trn _tgt_nobs_oot _tgt_nobs_trn_filt _tgt_nobs_oot_filt
        _tgt_has_col_train _tgt_has_col_oot;

    %if %length(%superq(target))=0 %then %do;
        %put ERROR: [target_contract] Variable target no definida.;
        %let _tgt_rc=1;
        %return;
    %end;

    %if %length(%superq(byvar))=0 %then %do;
        %put ERROR: [target_contract] Variable temporal (byvar) no definida.;
        %let _tgt_rc=1;
        %return;
    %end;

    %if %length(%superq(def_cld))=0 %then %do;
        %put ERROR: [target_contract] def_cld no definido.;
        %let _tgt_rc=1;
        %return;
    %end;

    %let _tgt_nobs_trn=0;
    %let _tgt_nobs_oot=0;

    proc sql noprint;
        select count(*) into :_tgt_nobs_trn trimmed
        from &input_caslib..&train_table.;

        select count(*) into :_tgt_nobs_oot trimmed
        from &input_caslib..&oot_table.;
    quit;

    %if &_tgt_nobs_trn.=0 %then %do;
        %put ERROR: [target_contract] TRAIN &input_caslib..&train_table. no accesible o vacia.;
        %let _tgt_rc=1;
        %return;
    %end;

    %if &_tgt_nobs_oot.=0 %then %do;
        %put ERROR: [target_contract] OOT &input_caslib..&oot_table. no accesible o vacia.;
        %let _tgt_rc=1;
        %return;
    %end;

    %_target_var_exists(data=&input_caslib..&train_table., var=&byvar.,
        outvar=_tgt_has_col_train);
    %_target_var_exists(data=&input_caslib..&oot_table., var=&byvar.,
        outvar=_tgt_has_col_oot);

    %if &_tgt_has_col_train.=0 or &_tgt_has_col_oot.=0 %then %do;
        %put ERROR: [target_contract] byvar=&byvar. debe existir en TRAIN y OOT.;
        %let _tgt_rc=1;
        %return;
    %end;

    %_target_var_exists(data=&input_caslib..&train_table., var=&target.,
        outvar=_tgt_has_col_train);
    %_target_var_exists(data=&input_caslib..&oot_table., var=&target.,
        outvar=_tgt_has_col_oot);

    %if &_tgt_has_col_train.=0 or &_tgt_has_col_oot.=0 %then %do;
        %put ERROR: [target_contract] target=&target. debe existir en TRAIN y OOT.;
        %let _tgt_rc=1;
        %return;
    %end;

    %let _tgt_nobs_trn_filt=0;
    %let _tgt_nobs_oot_filt=0;

    proc sql noprint;
        select count(*) into :_tgt_nobs_trn_filt trimmed
        from &input_caslib..&train_table.
        where &byvar. <= &def_cld.;

        select count(*) into :_tgt_nobs_oot_filt trimmed
        from &input_caslib..&oot_table.
        where &byvar. <= &def_cld.;
    quit;

    %if &_tgt_nobs_trn_filt.=0 %then %do;
        %put ERROR: [target_contract] TRAIN no tiene observaciones con &byvar. <= &def_cld.;
        %let _tgt_rc=1;
        %return;
    %end;

    %if &_tgt_nobs_oot_filt.=0 %then %do;
        %put ERROR: [target_contract] OOT no tiene observaciones con &byvar. <= &def_cld.;
        %let _tgt_rc=1;
        %return;
    %end;

    %if %length(%superq(monto_var)) > 0 %then %do;
        %_target_var_exists(data=&input_caslib..&train_table., var=&monto_var.,
            outvar=_tgt_has_col_train);
        %_target_var_exists(data=&input_caslib..&oot_table., var=&monto_var.,
            outvar=_tgt_has_col_oot);

        %if &_tgt_has_col_train.=1 and &_tgt_has_col_oot.=1 %then %do;
            %let _tgt_has_monto=1;
        %end;
        %else %do;
            %put WARNING: [target_contract] monto_var=&monto_var. no esta disponible en ambas muestras.;
            %put WARNING: [target_contract] Se omiten analisis ponderados.;
        %end;
    %end;
    %else %do;
        %put WARNING: [target_contract] monto_var no definido. Se omiten analisis ponderados.;
    %end;

    %put NOTE: [target_contract] OK - TRAIN total=&_tgt_nobs_trn. filtrado=&_tgt_nobs_trn_filt.;
    %put NOTE: [target_contract] OK - OOT total=&_tgt_nobs_oot. filtrado=&_tgt_nobs_oot_filt.;
    %put NOTE: [target_contract] target=&target. byvar=&byvar. def_cld=&def_cld. monto=&monto_var. has_monto=&_tgt_has_monto.;

%mend target_contract;
