class WebLoaderApp {
    constructor() {
        this.dockerStatus = null;
        this.lastUpdate = null;
        
        this.initializeEventListeners();
        this.loadDockerStatus();
        this.startAutoRefresh();
    }    initializeEventListeners() {
        // Formulario de consulta
        const queryForm = document.getElementById('queryForm');
        if (queryForm) {
            queryForm.addEventListener('submit', (e) => this.handleQuerySubmit(e));
        }

        // Selector de tipo de consulta
        const queryType = document.getElementById('queryType');
        if (queryType) {
            queryType.addEventListener('change', (e) => this.handleQueryTypeChange(e));
        }

        // Botón de reset
        const resetBtn = document.getElementById('resetQueryForm');
        if (resetBtn) {
            resetBtn.addEventListener('click', () => this.resetQueryForm());
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
            <span class="service-status ${statusClass}">
                ${serviceData.running ? 'Ejecutándose' : 
                serviceData.status === 'not_found' ? 'No encontrado' : 'Detenido'}
            </span>
            <i class="${serviceInfo.icon}"></i>
            <h3>${serviceInfo.name}</h3>
        `;
        
        return card;
    }    handleQueryTypeChange(event) {
        const selectedType = event.target.value;
        const configOptions = document.getElementById('queryConfigOptions');
        
        if (!selectedType) {
            configOptions.innerHTML = '<p>Selecciona un tipo de consulta para ver los parámetros</p>';
            configOptions.className = 'config-options';
            return;
        }

        configOptions.className = 'config-options active';
        
        // Generar opciones según el tipo de consulta
        const configs = this.getQueryConfigs(selectedType);
        configOptions.innerHTML = configs;
    }

    getQueryConfigs(type) {
        const configs = {
            'word_search': `
                <div class="config-item">
                    <label for="queryWord">Palabra a buscar:</label>
                    <input type="text" id="queryWord" name="word" placeholder="Ingresa una palabra..." required>
                    <p class="config-description">Función: getTopPagesByWord(palabra)</p>
                </div>
            `,
            'word_set2': `
                <div class="config-item">
                    <label>Par de palabras:</label>
                    <div class="input-row">
                        <input type="text" id="queryWord1" name="word1" placeholder="Primera palabra..." required>
                        <input type="text" id="queryWord2" name="word2" placeholder="Segunda palabra..." required>
                    </div>
                    <p class="config-description">Función: getTopPagesBySet2(palabra1, palabra2)</p>
                </div>
            `,
            'word_set3': `
                <div class="config-item">
                    <label>Tripleta de palabras:</label>
                    <div class="input-row">
                        <input type="text" id="queryWord1_3" name="word1" placeholder="Primera palabra..." required>
                        <input type="text" id="queryWord2_3" name="word2" placeholder="Segunda palabra..." required>
                        <input type="text" id="queryWord3_3" name="word3" placeholder="Tercera palabra..." required>
                    </div>
                    <p class="config-description">Función: getTopPagesBySet3(palabra1, palabra2, palabra3)</p>
                </div>
            `,
            'shared_bigrams': `
                <div class="config-item">
                    <label for="queryUrl">URL de la página:</label>
                    <input type="url" id="queryUrl" name="url" placeholder="https://ejemplo.com/pagina" required>
                    <p class="config-description">Función: getSharedBigramsByPage(url)</p>
                </div>
            `,
            'shared_trigrams': `
                <div class="config-item">
                    <label for="queryUrl2">URL de la página:</label>
                    <input type="url" id="queryUrl2" name="url" placeholder="https://ejemplo.com/pagina" required>
                    <p class="config-description">Función: getSharedTrigramsByPage(url)</p>
                </div>
            `,
            'page_words': `
                <div class="config-item">
                    <label for="queryUrl3">URL de la página:</label>
                    <input type="url" id="queryUrl3" name="url" placeholder="https://ejemplo.com/pagina" required>
                    <p class="config-description">Función: getDifferentWordsByPage(url)</p>
                </div>
            `,
            'page_links': `
                <div class="config-item">
                    <label for="queryUrl4">URL de la página:</label>
                    <input type="url" id="queryUrl4" name="url" placeholder="https://ejemplo.com/pagina" required>
                    <p class="config-description">Función: getLinkCountByPage(url)</p>
                </div>
            `,
            'page_percentages': `
                <div class="config-item">
                    <label for="queryUrl5">URL de la página:</label>
                    <input type="url" id="queryUrl5" name="url" placeholder="https://ejemplo.com/pagina" required>
                    <p class="config-description">Función: getPercentageWordsByPage(url)</p>
                </div>
            `,
            'word_repetitions': `
                <div class="config-item">
                    <label for="queryWordRep">Palabra a contar:</label>
                    <input type="text" id="queryWordRep" name="word" placeholder="Ingresa una palabra..." required>
                    <p class="config-description">Función: getTotalRepetitionsByWord(palabra)</p>
                </div>
            `,
            'page_repetitions': `
                <div class="config-item">
                    <label for="queryUrl6">URL de la página:</label>
                    <input type="url" id="queryUrl6" name="url" placeholder="https://ejemplo.com/pagina" required>
                    <p class="config-description">Función: getTotalRepetitionsByPage(url)</p>
                </div>
            `
        };

        return configs[type] || '<p>Configuración no disponible para este tipo de consulta</p>';
    }    async handleQuerySubmit(event) {
        event.preventDefault();
        
        const formData = new FormData(event.target);
        const queryType = formData.get('query_type');
        
        if (!queryType) {
            this.showModal('Error', 'Por favor selecciona un tipo de consulta');
            return;
        }

        // Mostrar que la consulta está siendo procesada
        const submitBtn = document.getElementById('executeQuery');
        const originalText = submitBtn.innerHTML;
        submitBtn.innerHTML = '<span class="loading"></span> Ejecutando consulta...';
        submitBtn.disabled = true;

        try {
            let endpoint = '';
            let params = {};
            
            // Construir endpoint según el tipo de consulta
            switch (queryType) {
                case 'word_search':
                    const word = formData.get('word');
                    if (!word) throw new Error('Palabra requerida');
                    endpoint = `/api/analysis/word/${encodeURIComponent(word)}`;
                    break;
                    
                case 'word_set2':
                    const word1 = formData.get('word1');
                    const word2 = formData.get('word2');
                    if (!word1 || !word2) throw new Error('Ambas palabras son requeridas');
                    endpoint = `/api/analysis/word-set2/${encodeURIComponent(word1)}/${encodeURIComponent(word2)}`;
                    break;
                    
                case 'word_set3':
                    const w1 = formData.get('word1');
                    const w2 = formData.get('word2');
                    const w3 = formData.get('word3');
                    if (!w1 || !w2 || !w3) throw new Error('Las tres palabras son requeridas');
                    endpoint = `/api/analysis/word-set3/${encodeURIComponent(w1)}/${encodeURIComponent(w2)}/${encodeURIComponent(w3)}`;
                    break;
                    
                case 'shared_bigrams':
                    const url1 = formData.get('url');
                    if (!url1) throw new Error('URL requerida');
                    endpoint = `/api/analysis/shared-bigrams?url=${encodeURIComponent(url1)}`;
                    break;
                    
                case 'shared_trigrams':
                    const url2 = formData.get('url');
                    if (!url2) throw new Error('URL requerida');
                    endpoint = `/api/analysis/shared-trigrams?url=${encodeURIComponent(url2)}`;
                    break;
                    
                case 'page_words':
                    const url3 = formData.get('url');
                    if (!url3) throw new Error('URL requerida');
                    endpoint = `/api/analysis/page-words?url=${encodeURIComponent(url3)}`;
                    break;
                    
                case 'page_links':
                    const url4 = formData.get('url');
                    if (!url4) throw new Error('URL requerida');
                    endpoint = `/api/analysis/page-links?url=${encodeURIComponent(url4)}`;
                    break;
                    
                case 'page_percentages':
                    const url5 = formData.get('url');
                    if (!url5) throw new Error('URL requerida');
                    endpoint = `/api/analysis/page-word-percentages?url=${encodeURIComponent(url5)}`;
                    break;
                    
                case 'word_repetitions':
                    const wordRep = formData.get('word');
                    if (!wordRep) throw new Error('Palabra requerida');
                    endpoint = `/api/analysis/word-repetitions/${encodeURIComponent(wordRep)}`;
                    break;
                    
                case 'page_repetitions':
                    const url6 = formData.get('url');
                    if (!url6) throw new Error('URL requerida');
                    endpoint = `/api/analysis/page-repetitions?url=${encodeURIComponent(url6)}`;
                    break;
                    
                default:
                    throw new Error('Tipo de consulta no válido');
            }

            const response = await fetch(endpoint);
            const result = await response.json();

            if (response.ok) {
                this.displayQueryResults(this.getQueryTitle(queryType, formData), result);
            } else {
                this.showModal('Error', result.error || 'Error al procesar la consulta');
            }

        } catch (error) {
            console.error('Error submitting query:', error);
            this.showModal('Error', error.message || 'Error de conexión al servidor');
        } finally {
            submitBtn.innerHTML = originalText;
            submitBtn.disabled = false;
        }
    }

    getQueryTitle(queryType, formData) {
        const titles = {
            'word_search': `Páginas que más contienen: "${formData.get('word')}"`,
            'word_set2': `Páginas con el par: "${formData.get('word1')}" y "${formData.get('word2')}"`,
            'word_set3': `Páginas con la tripleta: "${formData.get('word1')}", "${formData.get('word2')}" y "${formData.get('word3')}"`,
            'shared_bigrams': `Páginas con bigramas compartidos con: ${formData.get('url')}`,
            'shared_trigrams': `Páginas con trigramas compartidos con: ${formData.get('url')}`,
            'page_words': `Palabras distintas de: ${formData.get('url')}`,
            'page_links': `Conteo de links de: ${formData.get('url')}`,
            'page_percentages': `Porcentajes de palabras en: ${formData.get('url')}`,
            'word_repetitions': `Total repeticiones de: "${formData.get('word')}"`,
            'page_repetitions': `Total repeticiones de: ${formData.get('url')}`
        };
        
        return titles[queryType] || 'Resultados de la consulta';
    }    async loadResults() {
        try {
            const resultsSection = document.getElementById('resultsSection');
            const resultsContent = document.getElementById('resultsContent');
            
            resultsContent.innerHTML = '<div class="loading-container"><span class="loading"></span> Cargando resumen...</div>';
            resultsSection.style.display = 'block';

            // Cargar solo resumen
            const summaryResponse = await fetch('/api/results/summary');
            const summary = await summaryResponse.json();

            // Mostrar solo el resumen
            this.displaySummaryOnly(summary);

        } catch (error) {
            console.error('Error loading results:', error);
            this.showModal('Error', 'Error cargando resultados de la base de datos');
        }
    }

    displaySummaryOnly(summary) {
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
                    <div style="text-align: center; margin-top: 20px; padding: 20px; background: #f8f9fa; border-radius: 8px;">
                        <p style="color: #666; margin-bottom: 10px;">
                            <i class="fas fa-info-circle"></i> 
                            Para explorar datos específicos, utiliza la configuración de consulta.
                        </p>
                        <p style="color: #666;">
                            <i class="fas fa-database"></i> 
                            Selecciona un tipo de consulta y completa los parámetros para obtener resultados detallados.
                        </p>
                    </div>
                </div>
            </div>
        `;
    }displayQueryResults(title, result) {
        const resultsSection = document.getElementById('resultsSection');
        const resultsContent = document.getElementById('resultsContent');
        
        let content = `<h4>${title}</h4>`;
        
        if (result.error) {
            content += `<div class="sql-error">❌ ${result.error}</div>`;
        } else if (result.success && result.data) {
            const data = result.data;
            if (Array.isArray(data) && data.length > 0) {
                content += this.createSQLTable(data);
            } else {
                content += '<div class="sql-empty">No se encontraron resultados</div>';
            }
        } else {
            content += '<div class="sql-empty">No se encontraron resultados</div>';
        }
        
        resultsContent.innerHTML = content;
        resultsSection.style.display = 'block';
        
        // Scroll to results
        resultsSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }

    resetQueryForm() {
        const form = document.getElementById('queryForm');
        if (form) {
            form.reset();
            const configOptions = document.getElementById('queryConfigOptions');
            configOptions.innerHTML = '<p>Selecciona un tipo de consulta para ver los parámetros</p>';
            configOptions.className = 'config-options';
        }
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
            this.loadDockerStatus();        }, 30000);
    }

    // ...existing code...

    createSQLTable(data) {
        if (!data || data.length === 0) return '<div class="sql-empty">No hay datos disponibles</div>';
        
        // Obtener las keys del primer objeto para crear headers
        const keys = Object.keys(data[0]);
        
        let tableHTML = '<table class="sql-results-table"><thead><tr>';
        
        // Crear headers
        keys.forEach(key => {
            const headerName = key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
            tableHTML += `<th>${headerName}</th>`;
        });
        
        tableHTML += '</tr></thead><tbody>';
        
        // Crear filas
        data.forEach(row => {
            tableHTML += '<tr>';
            keys.forEach(key => {
                let value = row[key];
                // Formatear valores largos (URLs, títulos)
                if (typeof value === 'string' && value.length > 50) {
                    value = `<span title="${value}">${value.substring(0, 50)}...</span>`;
                }
                tableHTML += `<td>${value || '-'}</td>`;
            });
            tableHTML += '</tr>';
        });
        
        tableHTML += '</tbody></table>';
        return tableHTML;
    }

    showLoadingModal(message) {
        const modal = document.getElementById('modal');
        const modalTitle = document.getElementById('modalTitle');
        const modalMessage = document.getElementById('modalMessage');
        
        modalTitle.textContent = 'Procesando...';
        modalMessage.innerHTML = `<div class="loading-container"><span class="loading"></span> ${message}</div>`;
        modal.style.display = 'block';
    }

    async showAvailablePages() {
        this.showLoadingModal('Cargando páginas disponibles...');
        
        try {
            const response = await fetch('/api/helper/pages?limit=20');
            const result = await response.json();
            
            this.closeModal();
            
            if (result.success && result.data) {
                this.displayHelperData('Páginas Disponibles en la Base de Datos', result.data, 'pages');
            } else {
                this.showModal('Error', result.error || 'Error obteniendo páginas');
            }
            
        } catch (error) {
            this.closeModal();
            this.showModal('Error', 'Error al cargar páginas: ' + error.message);
        }
    }

    async showAvailableWords() {
        this.showLoadingModal('Cargando palabras populares...');
        
        try {
            const response = await fetch('/api/helper/words?limit=50');
            const result = await response.json();
            
            this.closeModal();
            
            if (result.success && result.data) {
                this.displayHelperData('Palabras Más Populares', result.data, 'words');
            } else {
                this.showModal('Error', result.error || 'Error obteniendo palabras');
            }
            
        } catch (error) {
            this.closeModal();
            this.showModal('Error', 'Error al cargar palabras: ' + error.message);
        }
    }    displayHelperData(title, data, type) {
        // Remover datos de ayuda anteriores
        const existingHelper = document.querySelector('.helper-data');
        if (existingHelper) {
            existingHelper.remove();
        }

        // Crear contenedor
        const helperContainer = document.createElement('div');
        helperContainer.className = 'helper-data sql-results';
        
        let content = `<h4>💡 ${title}</h4>`;
        
        if (type === 'pages') {
            content += '<div class="helper-grid">';
            data.forEach(page => {
                const shortUrl = page.url.length > 60 ? page.url.substring(0, 60) + '...' : page.url;
                const shortTitle = page.title && page.title.length > 40 ? page.title.substring(0, 40) + '...' : (page.title || 'Sin título');
                content += `
                    <div class="helper-item" onclick="app.fillUrl('${page.url}')">
                        <div class="helper-title">${shortTitle}</div>
                        <div class="helper-url">${shortUrl}</div>
                    </div>
                `;
            });
            content += '</div>';
            content += '<p class="helper-hint"><i class="fas fa-info-circle"></i> Haz clic en cualquier URL para copiarla al campo de consulta</p>';
        } else if (type === 'words') {
            content += '<div class="helper-words">';
            data.forEach(word => {
                content += `<span class="helper-word" onclick="app.fillWord('${word}')">${word}</span>`;
            });
            content += '</div>';
            content += '<p class="helper-hint"><i class="fas fa-info-circle"></i> Haz clic en cualquier palabra para usarla en las consultas</p>';
        }
        
        helperContainer.innerHTML = content;
        
        // Insertar después de la sección principal
        const mainSection = document.querySelector('.main-section');
        mainSection.parentNode.insertBefore(helperContainer, mainSection.nextSibling);
        
        // Scroll to helper data
        helperContainer.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }fillUrl(url) {
        // Buscar el primer campo de URL vacío
        const urlInputs = [
            'queryUrl', 'queryUrl2', 'queryUrl3', 'queryUrl4', 'queryUrl5', 'queryUrl6'
        ];
        
        for (let inputId of urlInputs) {
            const input = document.getElementById(inputId);
            if (input && !input.value.trim()) {
                input.value = url;
                this.showModal('✅ URL Copiada', `URL "${url}" ha sido copiada al campo de consulta`);
                return;
            }
        }
        
        // Si todos están llenos o no existen, mostrar mensaje
        this.showModal('💡 URL Disponible', `URL: "${url}" - Selecciona un tipo de consulta que requiera URL primero`);
    }

    fillWord(word) {
        // Buscar el primer campo de palabra vacío
        const wordInputs = [
            'queryWord', 'queryWordRep', 'queryWord1', 'queryWord1_3', 'queryWord2', 'queryWord2_3', 'queryWord3_3'
        ];
        
        for (let inputId of wordInputs) {
            const input = document.getElementById(inputId);
            if (input && !input.value.trim()) {
                input.value = word;
                this.showModal('✅ Palabra Copiada', `Palabra "${word}" ha sido copiada al campo de consulta`);
                return;
            }
        }
        
        // Si todos están llenos o no existen, mostrar mensaje
        this.showModal('💡 Palabra Disponible', `Palabra: "${word}" - Selecciona un tipo de consulta que requiera palabras primero`);
    }

    // ...existing code...
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
