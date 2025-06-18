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
                    <label>Número mínimo de repeticiones:</label>
                    <input type="number" name="min_frequency" value="2" min="1">
                </div>
                <div class="config-item">
                    <label>Palabras a excluir (separadas por coma):</label>
                    <input type="text" name="exclude_words" placeholder="el, la, de, que, y, ...">
                </div>
            `,
            'word_pairs': `
                <div class="config-item">
                    <label>Número mínimo de repeticiones del par:</label>
                    <input type="number" name="min_pair_frequency" value="2" min="1">
                </div>
            `,
            'word_triplets': `
                <div class="config-item">
                    <label>Número mínimo de repeticiones del triplet:</label>
                    <input type="number" name="min_triplet_frequency" value="2" min="1">
                </div>
            `,
            'page_similarity': `
                <div class="config-item">
                    <label>Umbral de similitud (0-1):</label>
                    <input type="number" name="similarity_threshold" value="0.7" min="0" max="1" step="0.1">
                </div>
            `,
            'link_analysis': `
                <div class="config-item">
                    <label>Tipo de enlaces a analizar:</label>
                    <select name="link_type">
                        <option value="all">Todos los enlaces</option>
                        <option value="internal">Solo internos</option>
                        <option value="external">Solo externos</option>
                    </select>
                </div>
            `,
            'custom': `
                <div class="config-item">
                    <label>Consulta personalizada Spark:</label>
                    <textarea name="custom_query" placeholder="Escribe tu consulta aquí..." rows="4"></textarea>
                </div>
            `
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
        submitBtn.innerHTML = '<span class="loading"></span> Procesando...';
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

    showAnalysisResult(result) {
        const resultsSection = document.getElementById('resultsSection');
        const resultsContent = document.getElementById('resultsContent');
        
        resultsContent.innerHTML = `
            <div class="analysis-result">
                <h3>✅ Análisis Enviado Correctamente</h3>
                <p><strong>ID de Análisis:</strong> ${result.analysis_id}</p>
                <p><strong>Estado:</strong> ${result.status}</p>
                <p><strong>Mensaje:</strong> ${result.message}</p>
                <div class="result-note">
                    <i class="fas fa-info-circle"></i>
                    <span>El análisis se está procesando en segundo plano. Los resultados se almacenarán en MariaDB una vez completado.</span>
                </div>
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

// Inicializar la aplicación cuando se carga la página
document.addEventListener('DOMContentLoaded', () => {
    new WebLoaderApp();
});

// Estilos para elementos de configuración
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
    
    .config-item textarea {
        resize: vertical;
        font-family: 'Courier New', monospace;
    }
    
    .analysis-result {
        text-align: center;
        padding: 20px;
    }
    
    .analysis-result h3 {
        color: var(--success-color);
        margin-bottom: 15px;
    }
    
    .analysis-result p {
        margin-bottom: 10px;
    }
    
    .result-note {
        margin-top: 20px;
        padding: 15px;
        background: #e3f2fd;
        border-radius: 8px;
        border-left: 4px solid var(--primary-color);
        display: flex;
        align-items: center;
        gap: 10px;
        text-align: left;
    }
    
    .result-note i {
        color: var(--primary-color);
        font-size: 1.2rem;
    }
`;

// Inyectar CSS adicional
const style = document.createElement('style');
style.textContent = additionalCSS;
document.head.appendChild(style);
