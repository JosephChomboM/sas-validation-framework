/* =========================================================================
   steps/01_setup_project.sas — Step 1: Setup del proyecto (FRONTEND)

   *** ESTE ES EL PUNTO DE ENTRADA DEL FRAMEWORK ***

   El usuario configura aquí la ruta raíz del proyecto.
   Al ejecutarse, crea automáticamente la estructura de carpetas
   necesaria si no existe (data/raw, data/processed, outputs/runs).

   Ref: design.md §5.1, README.md §5
   ========================================================================= */

/* ---- CONFIGURACIÓN DEL USUARIO (editar aquí) -------------------------- */

/*  _id_project_root  : Ruta absoluta al directorio raíz del proyecto       */
/*                       en el filesystem accesible por el servidor CAS.     */
/*                       Tipo: texto (ruta de filesystem)                    */
/*                       Ejemplo: /data/projects/framework_validacion        */
%let fw_root = /path/to/framework_validacion;

/* ---- EJECUCIÓN AUTOMÁTICA --------------------------------------------- */
/* Crea la estructura de carpetas base si no existe.                        */
/* No requiere sesión CAS — solo filesystem.                                */

options dlcreatedir;

libname _mkdir01 "&fw_root./data";
libname _mkdir01 clear;
libname _mkdir02 "&fw_root./data/raw";
libname _mkdir02 clear;
libname _mkdir03 "&fw_root./data/processed";
libname _mkdir03 clear;
libname _mkdir04 "&fw_root./outputs";
libname _mkdir04 clear;
libname _mkdir05 "&fw_root./outputs/runs";
libname _mkdir05 clear;

%put NOTE: ======================================================;
%put NOTE: [step-01] fw_root = &fw_root.;
%put NOTE: [step-01] Estructura de carpetas verificada/creada:;
%put NOTE:   &fw_root./data/raw/;
%put NOTE:   &fw_root./data/processed/;
%put NOTE:   &fw_root./outputs/runs/;
%put NOTE: ======================================================;
