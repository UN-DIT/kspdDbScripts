import {existsSync} from 'fs';
import {execSync, spawnSync} from 'child_process';
import path from 'path';
import {runWithLogging} from "../utils/runWithLogging";

const VENV_DIR = path.resolve(__dirname, '../../.venv');
const PYTHON_EXEC = process.platform === 'win32'
    ? path.join(VENV_DIR, 'Scripts', 'python.exe')
    : path.join(VENV_DIR, 'bin', 'python3');

const REQUIREMENTS = path.resolve(__dirname, '../../requirements.txt');
const PY_SCRIPT = path.resolve(__dirname, '../../src/scripts/normalizer.py');

function ensureVenv() {
    if (!existsSync(PYTHON_EXEC)) {
        console.log('🐍 Creating virtual environment...');
        execSync(`python3 -m venv ${VENV_DIR}`, {stdio: 'inherit'});
    } else {
        console.log('✅ venv already exists');
    }
}

function installDependencies() {
    if (existsSync(REQUIREMENTS)) {
        console.log('📦 Installing dependencies...');
        execSync(`${PYTHON_EXEC} -m pip install --upgrade pip`, {stdio: 'inherit'});
        execSync(`${PYTHON_EXEC} -m pip install -r ${REQUIREMENTS}`, {stdio: 'inherit'});
    } else {
        console.warn('⚠️ No requirements.txt found');
    }
}

function runPythonScript() {
    console.log(`🚀 Running: ${PY_SCRIPT}`);
    const result = spawnSync(PYTHON_EXEC, [PY_SCRIPT], {stdio: 'inherit'});

    if (result.error) {
        console.error('❌ Failed to run script:', result.error);
        process.exit(1);
    }
}

runWithLogging({
    script: {
        name: "️‍🐍 NORMALIZER [PYTHON]",
        index: 10,
        version: "1.0",
        text: "Нормалізація слів для пошуку"
    },
    run: async (db) => {
        try {
            ensureVenv();
            installDependencies();
            runPythonScript();
            console.log('✅ Done');
        } catch (err) {
            console.error('❌ Error during execution:', err);
            process.exit(1);
        }
    },
});
