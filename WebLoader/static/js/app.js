class WebLoaderApp {
    constructor() {
        this.dockerStatus = null;
        this.lastUpdate = null;
        
        this.initializeEventListeners();
        this.loadDockerStatus();
        this.startAutoRefresh();
    }

    initializeEventListeners() {
        // Formulario de análisis
        const analysisForm = document.getElementById('analysisForm');
        if (analysisForm) {
            analysisForm.addEventListener('submit', (e) => this.handleAnalysisSubmit(e));
        }

        // Selector de tipo de análisis
        const analysisType = document.getElementById('analysisType');
        if (analysisType) {
            analysisType.addEventListener('change', (e) => this.handleAnalysisTypeChange(e));
        }

        // Botón de reset
        const resetBtn = document.getElementById('resetForm');
        if (resetBtn) {
            resetBtn.addEventListener('click', () => this.resetForm());
        }

        // Botón para ver resultados
        const viewResultsBtn = document.getElementById('viewResults');
        if (viewResultsBtn) {
            viewResultsBtn.addEventListener('click', () => this.loadResults());
        }

        // Modal
        const closeModal = document.getElementById('closeModal');
        if (closeModal) {
            closeModal.addEventListener('click', () => this.closeModal());
        }

        window.addEventListener('click', (event) => {
            const modal = document.getElementById('modal');
            if (event.target === modal) {
                this.closeModal();
            }
        });
    }

    async loadDockerStatus() {
        try {
            const response = await fetch('/api/docker/status');
            const data = await response.json();
            
            this.dockerStatus = data;
            this.lastUpdate = new Date(data.timestamp);
            
            this.updateDockerStatusDisplay();
            this.updateServicesDisplay();
            this.updateLastUpdate();
            
        } catch (error) {
            console.error('Error loading Docker status:', error);
            this.showDockerError();
        }
    }

    updateDockerStatusDisplay() {
        const statusElement = document.getElementById('dockerStatus');
        const statusText = document.getElementById('dockerStatusText');
        
        if (this.dockerStatus && this.dockerStatus.docker_available) {
            statusElement.className = 'docker-status connected';
            statusText.textContent = 'Docker Conectado';
        } else {
            statusElement.className = 'docker-status disconnected';
            statusText.textContent = 'Docker No Disponible';
        }
    }

    updateServicesDisplay() {
        const servicesGrid = document.getElementById('servicesGrid');
        if (!servicesGrid || !this.dockerStatus) return;

        servicesGrid.innerHTML = '';

        const services = this.dockerStatus.services || {};
        const serviceNames = {
            'mysql': { name: 'MySQL', icon: 'fas fa-database' },
            'namenode': { name: 'Hadoop NameNode', icon: 'fas fa-server' },
            'spark': { name: 'Spark Master', icon: 'fas fa-fire' },
            'datanode': { name: 'Hadoop DataNode', icon: 'fas fa-hdd' },
            'resourcemanager': { name: 'YARN ResourceManager', icon: 'fas fa-layer-group' }
        };

        Object.entries(services).forEach(([serviceName, serviceData]) => {
            const serviceInfo = serviceNames[serviceName] || { name: serviceName, icon: 'fas fa-cog' };
            const serviceCard = this.createServiceCard(serviceInfo, serviceData);
            servicesGrid.appendChild(serviceCard);
        });
    }

    createServiceCard(serviceInfo, serviceData) {
        const card = document.createElement('div');
        const statusClass = serviceData.running ? 'running' : 
                           serviceData.status === 'not_found' ? 'unknown' : 'stopped';
        
        card.className = `service-card ${statusClass}`;
        card.innerHTML = `
            <i class="${serviceInfo.icon}"></i>
            <h3>${serviceInfo.name}</h3>
            <span class="service-status ${statusClass}">
                ${serviceData.running ? 'Ejecutándose' : 
                  serviceData.status === 'not_found' ? 'No encontrado' : 'Detenido'}
            </span>
        `;
        
        return card;
    }

    handleAnalysisTypeChange(event) {
        const selectedType = event.target.value;
        const configOptions = document.getElementById('configOptions');
        
        if (!selectedType) {
            configOptions.innerHTML = '<p>Selecciona un tipo de análisis para ver las opciones</p>';
            configOptions.className = 'config-options';
            return;
        }

        configOptions.className = 'config-options active';
        
        // Generar opciones según el tipo de análisis
        const configs = this.getAnalysisConfigs(selectedType);
        configOptions.innerHTML = configs;
    }

    getAnalysisConfigs(type) {
        const configs = {
            'word_frequency': `
                <div class="config-item">
                    <label>Ejecutar análisis completo de frecuencia de palabras:</label>
                    <p>Este análisis procesará todas las páginas web y calculará las frecuencias de palabras.</p>
                </div>
            `,
            'word_pairs': `
                <div class="config-item">
                    <label>Ejecutar análisis completo de pares de palabras:</label>
                    <p>Este análisis procesará todas las páginas web y calculará las frecuencias de pares de palabras.</p>
                </div>
            `,
            'word_triplets': `
                <div class="config-item">
                    <label>Ejecutar análisis completo de tripletas de palabras:</label>
                    <p>Este análisis procesará todas las páginas web y calculará las frecuencias de tripletas de palabras.</p>
                </div>
            `,
        };

        return configs[type] || '<p>Configuración no disponible para este tipo de análisis</p>';
    }

    async handleAnalysisSubmit(event) {
        event.preventDefault();
        
        const formData = new FormData(event.target);
        const analysisData = Object.fromEntries(formData.entries());
        
        // Validar que Docker esté disponible
        if (!this.dockerStatus || !this.dockerStatus.docker_available) {
            this.showModal('Error', 'Docker no está disponible. Por favor, inicia los contenedores necesarios.');
            return;
        }

        // Mostrar que el análisis está siendo procesado
        const submitBtn = document.getElementById('startAnalysis');
        const originalText = submitBtn.innerHTML;
        submitBtn.innerHTML = '<span class="loading"></span> Ejecutando Spark...';
        submitBtn.disabled = true;

        try {
            const response = await fetch('/api/analysis/submit', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(analysisData)
            });

            const result = await response.json();

            if (response.ok) {
                this.showAnalysisResult(result);
                // Cargar resultados después de 10 segundos
                setTimeout(() => this.loadResults(), 10000);
            } else {
                this.showModal('Error', result.error || 'Error al procesar el análisis');
            }

        } catch (error) {
            console.error('Error submitting analysis:', error);
            this.showModal('Error', 'Error de conexión al servidor');
        } finally {
            submitBtn.innerHTML = originalText;
            submitBtn.disabled = false;
        }
    }

    async loadResults() {
        try {
            const resultsSection = document.getElementById('resultsSection');
            const resultsContent = document.getElementById('resultsContent');
            
            resultsContent.innerHTML = '<div class="loading-container"><span class="loading"></span> Cargando resultados...</div>';
            resultsSection.style.display = 'block';

            // Cargar resumen
            const summaryResponse = await fetch('/api/results/summary');
            const summary = await summaryResponse.json();

            // Cargar resultados de palabras
            const wordsResponse = await fetch('/api/results/words?limit=10');
            const words = await wordsResponse.json();

            // Cargar resultados de pares
            const pairsResponse = await fetch('/api/results/word-pairs?limit=10');
            const pairs = await pairsResponse.json();

            // Cargar resultados de tripletas
            const tripletsResponse = await fetch('/api/results/word-triplets?limit=10');
            const triplets = await tripletsResponse.json();

            // Mostrar resultados
            this.displayResults(summary, words, pairs, triplets);

        } catch (error) {
            console.error('Error loading results:', error);
            this.showModal('Error', 'Error cargando resultados de la base de datos');
        }
    }

    displayResults(summary, words, pairs, triplets) {
        const resultsContent = document.getElementById('resultsContent');
        
        resultsContent.innerHTML = `
            <div class="results-container">
                <div class="results-summary">
                    <h3>📊 Resumen del Análisis</h3>
                    <div class="summary-grid">
                        <div class="summary-item">
                            <span class="summary-number">${summary.total_pages || 0}</span>
                            <span class="summary-label">Páginas Analizadas</span>
                        </div>
                        <div class="summary-item">
                            <span class="summary-number">${summary.total_words || 0}</span>
                            <span class="summary-label">Palabras Únicas</span>
                        </div>
                        <div class="summary-item">
                            <span class="summary-number">${summary.total_word_pairs || 0}</span>
                            <span class="summary-label">Pares de Palabras</span>
                        </div>
                        <div class="summary-item">
                            <span class="summary-number">${summary.total_word_triplets || 0}</span>
                            <span class="summary-label">Tripletas de Palabras</span>
                        </div>
                    </div>
                </div>

                <div class="results-tabs">
                    <div class="tab-buttons">
                        <button class="tab-btn active" onclick="showTab('words')">Palabras</button>
                        <button class="tab-btn" onclick="showTab('pairs')">Pares</button>
                        <button class="tab-btn" onclick="showTab('triplets')">Tripletas</button>
                    </div>

                    <div id="words-tab" class="tab-content active">
                        <h4>🔤 Top Palabras por Página</h4>
                        ${this.createResultsTable(words, ['word', 'page_title', 'quantity'], ['Palabra', 'Página', 'Frecuencia'])}
                    </div>

                    <div id="pairs-tab" class="tab-content">
                        <h4>🔗 Top Pares de Palabras por Página</h4>
                        ${this.createResultsTable(pairs, ['word_pair', 'page_title', 'repetition_count'], ['Par de Palabras', 'Página', 'Frecuencia'])}
                    </div>

                    <div id="triplets-tab" class="tab-content">
                        <h4>🎯 Top Tripletas de Palabras por Página</h4>
                        ${this.createResultsTable(triplets, ['word_triplet', 'page_title', 'repetition_count'], ['Tripleta de Palabras', 'Página', 'Frecuencia'])}
                    </div>
                </div>
            </div>
        `;
    }

    createResultsTable(data, columns, headers) {
        if (!data || data.length === 0) {
            return '<p>No hay datos disponibles</p>';
        }

        const tableRows = data.map(row => {
            const cells = columns.map(col => `<td>${row[col] || 'N/A'}</td>`).join('');
            return `<tr>${cells}</tr>`;
        }).join('');

        const headerCells = headers.map(h => `<th>${h}</th>`).join('');

        return `
            <table class="results-table">
                <thead>
                    <tr>${headerCells}</tr>
                </thead>
                <tbody>
                    ${tableRows}
                </tbody>
            </table>
        `;
    }

    showAnalysisResult(result) {
        const resultsSection = document.getElementById('resultsSection');
        const resultsContent = document.getElementById('resultsContent');
        
        resultsContent.innerHTML = `
            <div class="analysis-result">
                <h3>✅ Análisis de Spark Iniciado</h3>
                <p><strong>ID de Proceso:</strong> ${result.process_id}</p>
                <p><strong>Estado:</strong> ${result.status}</p>
                <p><strong>Mensaje:</strong> ${result.message}</p>
                <div class="result-note">
                    <i class="fas fa-info-circle"></i>
                    <span>El análisis se está procesando con Spark. Los resultados se almacenarán en MySQL automáticamente.</span>
                </div>
                <button class="btn btn-primary" onclick="app.loadResults()" style="margin-top: 15px;">
                    <i class="fas fa-refresh"></i> Ver Resultados
                </button>
            </div>
        `;
        
        resultsSection.style.display = 'block';
        resultsSection.classList.add('fade-in');
    }

    resetForm() {
        const form = document.getElementById('analysisForm');
        if (form) {
            form.reset();
            
            const configOptions = document.getElementById('configOptions');
            configOptions.innerHTML = '<p>Selecciona un tipo de análisis para ver las opciones</p>';
            configOptions.className = 'config-options';
            
            const resultsSection = document.getElementById('resultsSection');
            resultsSection.style.display = 'none';
        }
    }

    showDockerError() {
        const statusElement = document.getElementById('dockerStatus');
        const statusText = document.getElementById('dockerStatusText');
        
        statusElement.className = 'docker-status disconnected';
        statusText.textContent = 'Error de Conexión';
    }

    updateLastUpdate() {
        const element = document.getElementById('lastUpdate');
        if (this.lastUpdate && element) {
            element.textContent = this.lastUpdate.toLocaleString('es-ES');
        }
    }

    showModal(title, message) {
        document.getElementById('modalTitle').textContent = title;
        document.getElementById('modalMessage').innerHTML = message;
        document.getElementById('modal').style.display = 'block';
    }

    closeModal() {
        document.getElementById('modal').style.display = 'none';
    }

    startAutoRefresh() {
        // Actualizar estado de Docker cada 30 segundos
        setInterval(() => {
            this.loadDockerStatus();
        }, 30000);
    }
}

// Función global para cambiar tabs
function showTab(tabName) {
    // Ocultar todas las pestañas
    document.querySelectorAll('.tab-content').forEach(tab => {
        tab.classList.remove('active');
    });
    
    // Quitar clase active de todos los botones
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.remove('active');
    });
    
    // Mostrar la pestaña seleccionada
    document.getElementById(`${tabName}-tab`).classList.add('active');
    
    // Activar el botón correspondiente
    event.target.classList.add('active');
}

// Variable global para acceso desde HTML
let app;

// Inicializar la aplicación cuando se carga la página
document.addEventListener('DOMContentLoaded', () => {
    app = new WebLoaderApp();
});

// Estilos adicionales para las nuevas funcionalidades
const additionalCSS = `
    .config-item {
        margin-bottom: 15px;
    }
    
    .config-item label {
        display: block;
        margin-bottom: 5px;
        font-weight: 500;
        color: var(--dark-color);
    }
    
    .config-item input,
    .config-item select,
    .config-item textarea {
        width: 100%;
        padding: 8px 12px;
        border: 1px solid #ddd;
        border-radius: 6px;
        font-size: 0.9rem;
    }
    
    .analysis-result {
        text-align: center;
        padding: 20px;
    }
    
    .analysis-result h3 {
        color: var(--success-color);
        margin-bottom: 15px;
    }
    
    .results-container {
        max-width: 100%;
    }
    
    .results-summary {
        margin-bottom: 30px;
        padding: 20px;
        background: #f8f9fa;
        border-radius: 8px;
    }
    
    .summary-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
        gap: 20px;
        margin-top: 15px;
    }
    
    .summary-item {
        text-align: center;
        padding: 15px;
        background: white;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    
    .summary-number {
        display: block;
        font-size: 2rem;
        font-weight: bold;
        color: var(--primary-color);
    }
    
    .summary-label {
        font-size: 0.9rem;
        color: #666;
    }
    
    .results-tabs {
        background: white;
        border-radius: 8px;
        overflow: hidden;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    
    .tab-buttons {
        display: flex;
        background: #f8f9fa;
        border-bottom: 1px solid #ddd;
    }
    
    .tab-btn {
        flex: 1;
        padding: 15px;
        border: none;
        background: transparent;
        cursor: pointer;
        font-weight: 500;
        transition: background 0.3s;
    }
    
    .tab-btn.active {
        background: white;
        color: var(--primary-color);
        border-bottom: 2px solid var(--primary-color);
    }
    
    .tab-content {
        display: none;
        padding: 20px;
    }
    
    .tab-content.active {
        display: block;
    }
    
    .results-table {
        width: 100%;
        border-collapse: collapse;
        margin-top: 15px;
    }
    
    .results-table th,
    .results-table td {
        padding: 12px;
        text-align: left;
        border-bottom: 1px solid #ddd;
    }
    
    .results-table th {
        background: #f8f9fa;
        font-weight: 600;
        color: var(--dark-color);
    }
    
    .results-table tr:hover {
        background: #f8f9fa;
    }
    
    .loading-container {
        text-align: center;
        padding: 40px;
    }
`;

// Inyectar CSS adicional
const style = document.createElement('style');
style.textContent = additionalCSS;
document.head.appendChild(style);
