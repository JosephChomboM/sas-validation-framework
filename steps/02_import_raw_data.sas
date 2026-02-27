/* =========================================================================
   steps/02_import_raw_data.sas — Step 2: Importación de datos RAW (FRONTEND)

   El usuario configura aquí los parámetros de importación de datos
   desde Azure Data Lake Storage (ADLS).  Si adls_import_enabled=0,
   el framework salta la importación y asume que el .sashdat ya existe
   en data/raw/.

   Flujo: ADLS (parquet) → CAS → data/raw/<raw_table>.sashdat
   Este paso es PREVIO a la preparación de data processed.

   Ref: design.md §5.1, caslib_lifecycle.md
   ========================================================================= */

/* ---- CONFIGURACIÓN DEL USUARIO (editar aquí) -------------------------- */

/*  _id_import_enabled     : Habilitar/deshabilitar importación ADLS        */
/*                            0 = skip (asumir que .sashdat ya existe)       */
/*                            1 = importar parquet desde ADLS               */
%let adls_import_enabled = 1;

/*  _id_adls_storage       : Nombre de la cuenta Azure Data Lake Storage    */
/*                            Tipo: texto ($200)                             */
%let adls_storage = adlscu1cemmbackp05;

/*  _id_adls_container     : Filesystem/container dentro de ADLS            */
/*                            Tipo: texto ($200)                             */
%let adls_container = mi-container;

/*  _id_adls_parquet_path  : Ruta relativa del .parquet dentro del          */
/*                            container ADLS                                 */
/*                            Tipo: texto ($500)                             */
%let adls_parquet_path = data/modelo/dataset_v1.parquet;

/*  _id_raw_table_name     : Nombre del archivo .sashdat de salida          */
/*                            (sin extensión) en data/raw/                   */
/*                            Tipo: texto ($64)                              */
%let raw_table = mydataset;

/* ---- LOG DE CONFIRMACIÓN ---------------------------------------------- */

%put NOTE: ======================================================;
%put NOTE: [step-02] Configuración de importación ADLS:;
%put NOTE:   adls_import_enabled = &adls_import_enabled.;
%put NOTE:   adls_storage        = &adls_storage.;
%put NOTE:   adls_container      = &adls_container.;
%put NOTE:   adls_parquet_path   = &adls_parquet_path.;
%put NOTE:   raw_table           = &raw_table.;
%put NOTE: ======================================================;
