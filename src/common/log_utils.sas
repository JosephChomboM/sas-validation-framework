/* =========================================================================
log_utils.sas - Utilidades de logging por step via PROC PRINTTO
========================================================================= */

%macro _fw_ds_hasvar(data=, var=);
    %local _dsid _vnum _rc;
    %let _vnum=0;

    %if %sysfunc(exist(&data.)) %then %do;
        %let _dsid=%sysfunc(open(&data., i));
        %if &_dsid. > 0 %then %do;
            %let _vnum=%sysfunc(varnum(&_dsid., &var.));
            %let _rc=%sysfunc(close(&_dsid.));
        %end;
    %end;

    %if &_vnum. > 0 %then 1;
    %else 0;
%mend _fw_ds_hasvar;

%macro _fw_append_audit_row(step_name=, step_rc=0, step_status=OK);
    %local _fw_audit_path _fw_audit_table _fw_tipo_wf _fw_workflow_name
        _fw_status _fw_end_dttm _fw_user_id _fw_nombre_prueba _fw_dataset_name
        _fw_tipo_modelo _fw_tipo_producto _fw_wf_version _fw_troncal _fw_segmento
        _fw_scope _fw_split _fw_cfg_tipo_modelo _fw_cfg_model_type
        _fw_cfg_tipo_producto _fw_step_group _fw_module_name _fw_skip_flag;

    %let _fw_end_dttm=%sysfunc(datetime());

    %if %symexist(_fw_log_start_dttm)=0 or %superq(_fw_log_start_dttm)= %then
        %let _fw_log_start_dttm=&_fw_end_dttm.;

    %if %symexist(fw_audit_path)=1 and %superq(fw_audit_path) ne %then
        %let _fw_audit_path=&fw_audit_path.;
    %else
        %let _fw_audit_path=/bcp/bcp-exploratorio-adr-vime/transform_vi_monitoring/monitoring_workflow_scoring_vi;

    %if %symexist(fw_audit_table)=1 and %superq(fw_audit_table) ne %then
        %let _fw_audit_table=&fw_audit_table.;
    %else
        %let _fw_audit_table=auditoria_ejecuciones_v3;

    %let _fw_tipo_wf=scoring;
    %let _fw_workflow_name=framework_validacion;
    %let _fw_status=%upcase(%superq(step_status));
    %if %superq(_fw_status)= %then %let _fw_status=OK;
    %if %sysevalf(&step_rc. ne 0) %then %let _fw_status=ERROR;
    %let _fw_skip_flag=%sysfunc(ifc(%upcase(&_fw_status.)=SKIP,1,0));

    %let _fw_user_id=%scan(&SYSUSERID., 1, @);

    %let _fw_nombre_prueba=;
    %if %symexist(nombre_prueba)=1 and %superq(nombre_prueba) ne %then
        %let _fw_nombre_prueba=%superq(nombre_prueba);
    %else %if %symexist(_id_nombre_prueba)=1 and %superq(_id_nombre_prueba) ne %then
        %let _fw_nombre_prueba=%superq(_id_nombre_prueba);
    %else %if %symexist(fw_sas_dataset_name)=1 and %superq(fw_sas_dataset_name) ne %then
        %let _fw_nombre_prueba=%superq(fw_sas_dataset_name);
    %else
        %let _fw_nombre_prueba=&_fw_log_run.;

    %let _fw_dataset_name=;
    %if %symexist(fw_sas_dataset_name)=1 and %superq(fw_sas_dataset_name) ne %then
        %let _fw_dataset_name=%superq(fw_sas_dataset_name);

    %let _fw_wf_version=;
    %if %symexist(wf_version)=1 and %superq(wf_version) ne %then
        %let _fw_wf_version=%superq(wf_version);
    %else %if %symexist(_id_wf_version)=1 and %superq(_id_wf_version) ne %then
        %let _fw_wf_version=%superq(_id_wf_version);
    %else
        %let _fw_wf_version=v1;

    %let _fw_troncal=;
    %if %symexist(ctx_troncal_id)=1 %then %let _fw_troncal=%superq(ctx_troncal_id);
    %let _fw_segmento=;
    %if %symexist(ctx_seg_id)=1 %then %let _fw_segmento=%superq(ctx_seg_id);
    %let _fw_scope=;
    %if %symexist(ctx_scope)=1 %then %let _fw_scope=%superq(ctx_scope);
    %let _fw_split=;
    %if %symexist(ctx_split)=1 %then %let _fw_split=%superq(ctx_split);

    %let _fw_cfg_tipo_modelo=;
    %let _fw_cfg_model_type=;
    %let _fw_cfg_tipo_producto=;

    %if %superq(_fw_troncal) ne %then %do;
        %if %_fw_ds_hasvar(data=casuser.cfg_troncales, var=tipo_modelo) %then %do;
            proc sql noprint outobs=1;
                select strip(tipo_modelo)
                  into :_fw_cfg_tipo_modelo trimmed
                  from casuser.cfg_troncales
                 where troncal_id=&_fw_troncal.;
            quit;
        %end;
        %if %_fw_ds_hasvar(data=casuser.cfg_troncales, var=model_type) %then %do;
            proc sql noprint outobs=1;
                select strip(model_type)
                  into :_fw_cfg_model_type trimmed
                  from casuser.cfg_troncales
                 where troncal_id=&_fw_troncal.;
            quit;
        %end;
        %if %_fw_ds_hasvar(data=casuser.cfg_troncales, var=tipo_producto) %then %do;
            proc sql noprint outobs=1;
                select strip(tipo_producto)
                  into :_fw_cfg_tipo_producto trimmed
                  from casuser.cfg_troncales
                 where troncal_id=&_fw_troncal.;
            quit;
        %end;
    %end;

    %let _fw_tipo_modelo=;
    %if %symexist(tipo_modelo)=1 and %superq(tipo_modelo) ne %then
        %let _fw_tipo_modelo=%superq(tipo_modelo);
    %else %if %symexist(model_type)=1 and %superq(model_type) ne %then
        %let _fw_tipo_modelo=%superq(model_type);
    %else %if %superq(_fw_cfg_tipo_modelo) ne %then
        %let _fw_tipo_modelo=&_fw_cfg_tipo_modelo.;
    %else %if %superq(_fw_cfg_model_type) ne %then
        %let _fw_tipo_modelo=&_fw_cfg_model_type.;

    %let _fw_tipo_producto=;
    %if %symexist(tipo_producto)=1 and %superq(tipo_producto) ne %then
        %let _fw_tipo_producto=%superq(tipo_producto);
    %else %if %superq(_fw_cfg_tipo_producto) ne %then
        %let _fw_tipo_producto=&_fw_cfg_tipo_producto.;

    %let _fw_step_group=CORE;
    %if %upcase(%substr(%superq(_fw_log_stem), 1, 6))=METOD_ %then
        %let _fw_step_group=METHOD;

    %let _fw_module_name=%scan(%superq(_fw_log_stem), -1, _);

    data work._fw_audit_row;
        length fecha_ejecucion 8 hora_inicio 8 hora_fin 8 duracion_minutos 8
            duracion_segundos 8 step_rc 8 skip_flag 8 success_flag 8
            user_id $128 nombre_prueba $256 tipo_modelo $64 tipo_producto $64
            troncal $32 segmento $32 wf_version $64 tipo_wf $32
            run_id $64 step_name $128 log_stem $128 log_path $512
            step_status $16 ctx_scope $32 ctx_split $32 step_group $16
            module_name $64 workflow_name $64 dataset_name $128
            host_name $128 sas_userid $256;
        format fecha_ejecucion yymmdd10. hora_inicio time8. hora_fin time8.
            duracion_minutos 12.2 duracion_segundos 12.2;

        fecha_ejecucion=datepart(&_fw_log_start_dttm.);
        hora_inicio=timepart(&_fw_log_start_dttm.);
        hora_fin=timepart(&_fw_end_dttm.);
        duracion_segundos=round(&_fw_end_dttm. - &_fw_log_start_dttm., 0.01);
        duracion_minutos=round(duracion_segundos / 60, 0.01);
        step_rc=&step_rc.;
        skip_flag=&_fw_skip_flag.;
        success_flag=(step_rc=0 and skip_flag=0);
        user_id=symget('_fw_user_id');
        nombre_prueba=symget('_fw_nombre_prueba');
        tipo_modelo=symget('_fw_tipo_modelo');
        tipo_producto=symget('_fw_tipo_producto');
        troncal=symget('_fw_troncal');
        segmento=symget('_fw_segmento');
        wf_version=symget('_fw_wf_version');
        tipo_wf=symget('_fw_tipo_wf');
        run_id=symget('_fw_log_run');
        step_name=symget('_fw_log_step');
        log_stem=symget('_fw_log_stem');
        log_path=symget('_fw_log_path');
        step_status=symget('_fw_status');
        ctx_scope=symget('_fw_scope');
        ctx_split=symget('_fw_split');
        step_group=symget('_fw_step_group');
        module_name=symget('_fw_module_name');
        workflow_name=symget('_fw_workflow_name');
        dataset_name=symget('_fw_dataset_name');
        host_name=symget('SYSHOSTNAME');
        sas_userid=symget('SYSUSERID');
    run;

    libname _fwaud "&_fw_audit_path.";

    %if %sysfunc(libref(_fwaud)) ne 0 %then %do;
        %put WARNING: [fw_log_stop] No se pudo asignar libname de auditoria en &_fw_audit_path..;
        %goto _fw_audit_done;
    %end;

    %if %sysfunc(exist(_fwaud.&_fw_audit_table.)) %then %do;
        proc append base=_fwaud.&_fw_audit_table.
            data=work._fw_audit_row force;
        run;
    %end;
    %else %do;
        data _fwaud.&_fw_audit_table.;
            set work._fw_audit_row;
        run;
    %end;

    %put NOTE: [fw_log_stop] Auditoria registrada en &_fw_audit_path./&_fw_audit_table..sas7bdat;

%_fw_audit_done:
    %if %sysfunc(libref(_fwaud))=0 %then %do;
        libname _fwaud clear;
    %end;

    proc datasets library=work nolist nowarn;
        delete _fw_audit_row;
    quit;
%mend _fw_append_audit_row;

%macro fw_log_start(step_name=, run_id=, fw_root=, log_stem=);
    %global _fw_log_path _fw_log_step _fw_log_run _fw_log_stem
        _fw_log_start_dttm;

    %let _fw_log_step=&step_name.;
    %let _fw_log_run=&run_id.;
    %let _fw_log_stem=&log_stem.;
    %let _fw_log_start_dttm=%sysfunc(datetime());
    %let _fw_log_path=&fw_root./outputs/runs/&run_id./logs/&log_stem..log;

    proc printto log="&_fw_log_path." new;
    run;

    %put NOTE:======================================================;
    %put NOTE: [fw_log_start] step=&step_name.;
    %put NOTE: [fw_log_start] run_id=&run_id.;
    %put NOTE: [fw_log_start] log=&_fw_log_path.;
    %put NOTE: [fw_log_start] started_at=%sysfunc(datetime(), E8601DT19.).;
    %if %symexist(ctx_scope)=1 %then
        %put NOTE: [fw_log_start] ctx_scope=&ctx_scope.;
    %if %symexist(ctx_troncal_id)=1 %then
        %put NOTE: [fw_log_start] ctx_troncal_id=&ctx_troncal_id.;
    %if %symexist(ctx_split)=1 %then
        %put NOTE: [fw_log_start] ctx_split=&ctx_split.;
    %if %symexist(ctx_seg_id)=1 %then
        %put NOTE: [fw_log_start] ctx_seg_id=&ctx_seg_id.;
    %put NOTE:======================================================;
%mend fw_log_start;

%macro fw_log_stop(step_name=, step_rc=0, step_status=OK);
    %put NOTE:======================================================;
    %put NOTE: [fw_log_stop] step=&step_name.;
    %put NOTE: [fw_log_stop] step_rc=&step_rc.;
    %put NOTE: [fw_log_stop] step_status=&step_status.;
    %put NOTE: [fw_log_stop] finished_at=%sysfunc(datetime(), E8601DT19.).;
    %put NOTE:======================================================;

    %_fw_append_audit_row(step_name=&step_name., step_rc=&step_rc.,
        step_status=&step_status.);

    proc printto;
    run;
%mend fw_log_stop;
