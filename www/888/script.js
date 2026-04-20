document.addEventListener('DOMContentLoaded', () => {

    // --- CONSTANTES DE CHAVES E ELEMENTOS ---
    const ANALYTICS_KEY = 'geradorWhatsAnalytics';
    const HISTORY_KEY = 'geradorWhatsHistory';
    const CUSTOM_TEMPLATES_KEY = 'geradorWhatsCustomTemplates';
    const MAX_HISTORY_ITEMS = 10;

    // --- RASTREIO DE CLIQUES (DEVE EXECUTAR PRIMEIRO) ---
    const checkForRedirection = () => {
        const hash = window.location.hash;
        if (hash.startsWith('#/track/')) {
            const linkId = hash.substring(8);
            const links = JSON.parse(localStorage.getItem(ANALYTICS_KEY)) || [];
            const linkIndex = links.findIndex(l => l.id === linkId);
            
            if (linkIndex > -1) {
                links[linkIndex].clicks++;
                localStorage.setItem(ANALYTICS_KEY, JSON.stringify(links));
                
                setTimeout(() => {
                    window.location.hash = '';
                    window.location.href = links[linkIndex].destinationUrl;
                }, 50);
            }
        }
    };
    checkForRedirection();

    // --- SELEÇÃO DE ELEMENTOS DO DOM ---
    const body = document.body;
    const linkForm = document.getElementById('link-form');
    const numeroInput = document.getElementById('numero-whatsapp');
    const numeroVCardInput = document.getElementById('numero-vcard');
    const mensagemInput = document.getElementById('mensagem');
    const templateCategorySelect = document.getElementById('template-category');
    const templateListDiv = document.getElementById('template-list');
    const abrirWppBtn = document.getElementById('abrir-wpp-btn');
    const limparBtn = document.getElementById('limpar-btn');
    const copiarBtn = document.getElementById('copiar-btn');
    const resultArea = document.getElementById('result-area');
    const linkGeradoA = document.getElementById('link-gerado');
    const qrCodeContainer = document.getElementById('qrcode-container');
    const numeroWhatsappErrorSpan = document.getElementById('numero-whatsapp-error');
    const numeroVCardErrorSpan = document.getElementById('numero-vcard-error');
    const previewText = document.getElementById('preview-text');
    const previewTimestamp = document.getElementById('preview-timestamp');
    const previewContactName = document.getElementById('preview-contact-name');
    const themeToggle = document.getElementById('theme-toggle');
    const qrDotColorInput = document.getElementById('qr-dot-color');
    const qrBgColorInput = document.getElementById('qr-bg-color');
    const qrLogoUpload = document.getElementById('qr-logo-upload');
    const qrDownloadBtn = document.getElementById('qr-download-btn');
    const qrDownloadFormat = document.getElementById('qr-download-format');
    const historyListContainer = document.getElementById('history-list-container');
    const historyItemTemplate = document.getElementById('history-item-template');
    const formatBoldBtn = document.getElementById('format-bold');
    const formatItalicBtn = document.getElementById('format-italic');
    const formatStrikeBtn = document.getElementById('format-strike');
    const customTemplateForm = document.getElementById('custom-template-form');
    const customTemplateTitleInput = document.getElementById('custom-template-title');
    const customTemplateTextInput = document.getElementById('custom-template-text');
    const customTemplateListContainer = document.getElementById('custom-template-list-container');
    const customTemplateItemTemplate = document.getElementById('custom-template-item-template');
    const generatorTypeRadios = document.querySelectorAll('input[name="generatorType"]');
    const vcardFieldsContainer = document.getElementById('vcard-fields-container');
    const whatsappFieldsContainer = document.getElementById('whatsapp-fields-container');
    const vcardFirstNameInput = document.getElementById('vcard-firstname');
    const vcardMiddleNameInput = document.getElementById('vcard-middlename');
    const vcardLastNameInput = document.getElementById('vcard-lastname');
    const vcardNicknameInput = document.getElementById('vcard-nickname');
    const vcardPhotoUpload = document.getElementById('vcard-photo-upload');
    const vcardPhotoPreview = document.getElementById('vcard-photo-preview');
    const photoWarning = document.getElementById('photo-warning');
    const vcardEmailInput = document.getElementById('vcard-email');
    const vcardCompanyInput = document.getElementById('vcard-company');
    const vcardTitleInput = document.getElementById('vcard-title');
    const vcardWebsiteInput = document.getElementById('vcard-website');
    const vcardLinkedinInput = document.getElementById('vcard-linkedin');
    const vcardInstagramInput = document.getElementById('vcard-instagram');
    const vcardTwitterInput = document.getElementById('vcard-twitter');
    const vcardGithubInput = document.getElementById('vcard-github');
    const vcardTelegramInput = document.getElementById('vcard-telegram');
    const vcardYoutubeInput = document.getElementById('vcard-youtube');
    const vcardRedditInput = document.getElementById('vcard-reddit');
    const vcardAddressInput = document.getElementById('vcard-address');
    const vcardCityInput = document.getElementById('vcard-city');
    const vcardNotesInput = document.getElementById('vcard-notes');
    const messageGroup = document.getElementById('message-group');
    const trackingGroup = document.getElementById('tracking-group');
    const trackClicksCheckbox = document.getElementById('track-clicks-checkbox');
    const linkNameGroup = document.getElementById('link-name-group');
    const linkNameInput = document.getElementById('link-name');
    const linkResultGroup = document.getElementById('link-result-group');
    const gerarBtnText = document.getElementById('gerar-btn-text');
    const analyticsListContainer = document.getElementById('analytics-list-container');
    const analyticsItemTemplate = document.getElementById('analytics-item-template');
    const exportDataBtn = document.getElementById('export-data-btn');
    const importDataInput = document.getElementById('import-data-input');
    const variableInputsContainer = document.getElementById('variable-inputs-container');
    const variableInputTemplate = document.getElementById('variable-input-template');
    const linkPreviewContainer = document.getElementById('link-preview-container');
    const linkPreviewImage = document.getElementById('link-preview-image');
    const linkPreviewTitle = document.getElementById('link-preview-title');
    const linkPreviewDescription = document.getElementById('link-preview-description');
    const linkPreviewUrl = document.getElementById('link-preview-url');

    // --- VARIÁVEIS GLOBAIS ---
    let qrCodeInstance = null;
    let currentGeneratorType = 'whatsapp';
    let masterTemplateText = '';
    let linkPreviewDebounce;
    let itiWhatsapp, itiVCard;
    let vcardPhotoBase64 = '';

    // --- DADOS DE TEMPLATES PRÉ-DEFINIDOS ---
    const templates = {
        pessoal: [ { title: 'Convite de Aniversário', text: 'Olá, {{nome}}! 🎉 Gostaria de te convidar para a minha festa de aniversário no dia {{data}}, às {{hora}}, em {{local}}. A tua presença seria o melhor presente! Confirma se vens. Abraço!' }, { title: 'Lembrete de Compromisso', text: 'Oi, {{nome}}! A passar para lembrar do nosso café/reunião amanhã às {{hora}}. Até lá!' }, { title: 'Partilha de Localização', text: 'Olá! Já estou em {{local_atual}}. Segue a minha localização para nos encontrarmos. Avisa quando estiveres a chegar!' }, { title: 'Agradecimento por Presente', text: 'Olá, {{nome}}! Adorei o presente que me deste, muito obrigado(a) pelo carinho e por te lembrares! Significou muito para mim.' }, { title: 'Agradecimento por Ajuda', text: 'Queria agradecer imensamente pela tua ajuda com {{assunto}}. Salvaste o meu dia! Fico a dever-te uma.' }, { title: 'Organizar Viagem', text: 'Pessoal, a pensar em organizarmos aquela viagem para {{destino}} no fim de semana de {{data}}. Quem está dentro? Vamos combinar os detalhes.' }, { title: 'Pedido de Empréstimo', text: 'Olá, {{nome}}, tudo bem? Será que terias {{objeto}} para me emprestar por uns dias? Devolvo assim que terminar de usar. Obrigado!' }, { title: 'Parabéns por Conquista', text: 'Muitos parabéns pela tua nova conquista, {{conquista}}! Fiquei super feliz por ti. Mereces todo o sucesso! Vamos comemorar!' }, { title: 'Aviso de "A Caminho"', text: 'Estou a sair de casa agora, devo chegar em cerca de {{tempo_estimado}} minutos. Até já!' }, { title: 'Pedido de Opinião', text: 'Oi, {{nome}}! Estou a pensar em {{assunto}} e valorizo muito a tua opinião. O que achas sobre isto? Qualquer ideia é bem-vinda.' }, { title: 'Marcar Encontro', text: 'Olá! Que saudades. Estava a pensar em marcarmos um jantar para pôr a conversa em dia. Teria disponibilidade na próxima {{dia_da_semana}}?' }, { title: 'Notícia Urgente', text: 'Atenção, pessoal! Notícia importante sobre {{assunto}}. Por favor, leiam e respondam assim que puderem.' }, { title: 'Verificar Amigo', text: 'Oi, {{nome}}, há algum tempo que não falamos. Só a mandar uma mensagem para saber se está tudo bem contigo. Abraço!' }, { title: 'Pedido de Contacto', text: 'Olá! Perdi o contacto do(a) {{nome_da_pessoa}}. Por acaso não o(a) tens para me enviares? Obrigado!' }, { title: 'Convidar para Cinema/Série', text: 'E aí, {{nome}}! Vi que o filme/série {{nome_do_filme}} estreou. Que tal combinarmos para assistir juntos esta semana?' } ],
        empreendedor: [ { title: 'Networking Pós-Evento', text: 'Olá, {{nome}}! Foi um prazer conhecer-te no evento {{nome_do_evento}}. Adoraria conectar-me e, quem sabe, explorarmos sinergias futuras. O que achas?' }, { title: 'Envio de Orçamento', text: 'Prezado(a) {{nome_cliente}}, conforme conversámos, segue em anexo a nossa proposta para o serviço de {{servico}}. Fico à inteira disposição para esclarecer qualquer dúvida.' }, { title: 'Follow-up de Orçamento', text: 'Olá, {{nome_cliente}}, tudo bem? Gostaria de saber se tiveste a oportunidade de analisar a proposta que enviei. Há algo mais em que eu possa ajudar?' }, { title: 'Agendamento de Reunião', text: 'Olá, {{nome}}. Para discutirmos melhor o projeto {{nome_do_projeto}}, sugiro uma breve chamada. Teria disponibilidade na {{dia_da_semana}} às {{hora1}} ou {{hora2}}?' }, { title: 'Feedback Pós-Serviço', text: 'Olá, {{nome_cliente}}! Espero que estejas a gostar do nosso trabalho. O teu feedback é muito valioso. Poderias deixar um breve depoimento sobre a tua experiência?' }, { title: 'Anúncio de Promoção', text: 'Olá, cliente amigo! Temos uma promoção imperdível no nosso serviço/produto {{nome_do_produto}} apenas esta semana. Não percas! Gostaria de saber mais?' }, { title: 'Pedido de Indicação', text: 'Olá, {{nome_cliente}}! Fico feliz que tenhas gostado do nosso trabalho. Se conheceres alguém que também poderia beneficiar dos nossos serviços, ficaríamos muito gratos pela indicação.' }, { title: 'Prospeção Fria', text: 'Olá, {{nome}}! Vi que também fazes parte do grupo {{nome_do_grupo}} e reparei no teu excelente trabalho em {{area}}. Gostaria de apresentar como a minha solução de {{minha_solucao}} poderia ajudar-te.' }, { title: 'Aviso de Pagamento', text: 'Prezado(a) {{nome_cliente}}, este é um lembrete amigável sobre a fatura n.º {{numero_fatura}}, com vencimento em {{data_vencimento}}. Obrigado!' }, { title: 'Apresentação de Novo Serviço', text: 'Olá, {{nome_cliente}}! Como um cliente valioso, gostaria de apresentar em primeira mão o nosso novo serviço de {{novo_servico}}, que pode ser do teu interesse. Queres saber mais?' }, { title: 'Reativar Cliente Antigo', text: 'Olá, {{nome}}! Já há algum tempo que não conversamos. Lembrei-me de ti e gostaria de saber se há algo novo em que te possa ajudar. Temos novidades!' }, { title: 'Confirmação de Agendamento', text: 'Olá! A confirmar o nosso agendamento para o dia {{data}} às {{hora}}. Se precisares de reagendar, por favor, avisa com antecedência. Obrigado!' }, { title: 'Resposta Automática (Ausência)', text: 'Olá! Agradeço o teu contacto. De momento estou fora do meu horário de trabalho, mas responderei à tua mensagem assim que possível amanhã de manhã. Obrigado pela compreensão.' }, { title: 'Convite para Webinar/Workshop', text: 'Olá, {{nome}}! Convido-te a participar no nosso webinar gratuito sobre {{tema_do_webinar}} no dia {{data}}. Será uma ótima oportunidade para aprender mais. Inscreve-te aqui: {{link}}' }, { title: 'Agradecimento por Parceria', text: 'Olá, {{nome_parceiro}}! Gostaria de agradecer pela parceria de sucesso no projeto {{nome_do_projeto}}. Espero que possamos colaborar novamente em breve!' } ],
        empresas: [ { title: 'Boas-vindas a Novo Cliente', text: 'Prezado(a) {{nome_do_cliente}}, em nome de toda a equipa da {{nome_da_empresa}}, damos-lhe as boas-vindas! Estamos muito felizes por tê-lo(a) como nosso cliente.' }, { title: 'Suporte (Abertura de Ticket)', text: 'Olá! Recebemos a sua solicitação de suporte. O seu ticket é o n.º {{numero_ticket}}. Um dos nossos especialistas entrará em contacto em breve. Obrigado.' }, { title: 'Suporte (Resolução)', text: 'Informamos que a sua solicitação (Ticket {{numero_ticket}}) foi resolvida. Se precisar de mais alguma coisa, não hesite em contactar-nos. A {{nome_da_empresa}} agradece.' }, { title: 'Pesquisa de Satisfação (NPS)', text: 'Olá, {{nome_do_cliente}}. Numa escala de 0 a 10, qual a probabilidade de você recomendar a {{nome_da_empresa}} a um amigo ou colega? A sua opinião é fundamental para nós.' }, { title: 'Aviso de Manutenção', text: 'Aviso: No dia {{data}}, entre as {{hora_inicio}} e as {{hora_fim}}, os nossos serviços estarão em manutenção para melhorias. Pedimos desculpa por qualquer inconveniente.' }, { title: 'Confirmação de Encomenda', text: 'A sua encomenda n.º {{numero_encomenda}} foi confirmada e está a ser preparada para envio. Acompanharemos com mais detalhes em breve. Obrigado pela sua compra!' }, { title: 'Recuperação de Carrinho', text: 'Olá, {{nome_do_cliente}}! Reparámos que deixou alguns itens no seu carrinho na nossa loja. Gostaria de finalizar a sua compra? O seu carrinho está à sua espera.' }, { title: 'Comunicação de Crise', text: 'Atenção: Estamos cientes de um problema que afeta {{servico_afetado}}. A nossa equipa técnica já está a trabalhar na resolução com prioridade máxima. Iremos atualizando.' }, { title: 'Divulgação de Vaga', text: 'Estamos a contratar! A {{nome_da_empresa}} tem uma vaga aberta para {{cargo}}. Se conhece o candidato ideal, partilhe! Mais detalhes em: {{link_da_vaga}}' }, { title: 'Confirmação de Subscrição', text: 'Bem-vindo(a) à nossa newsletter! Confirmamos a sua subscrição. Prepare-se para receber as melhores dicas e novidades sobre {{tema}}.' }, { title: 'Aviso de Termos de Serviço', text: 'Informamos que os nossos Termos de Serviço e Política de Privacidade foram atualizados. Pode consultá-los em {{link_dos_termos}}. Obrigado por fazer parte da nossa comunidade.' }, { title: 'Convite para Programa Beta', text: 'Olá, cliente especial! Estamos a convidar um grupo selecionado para testar em primeira mão a nossa nova funcionalidade: {{nome_da_funcionalidade}}. Teria interesse em ser um testador beta?' }, { title: 'Gestão de Reclamações', text: 'Prezado(a) {{nome_do_cliente}}, recebemos a sua reclamação e lamentamos o sucedido. Estamos a analisar a sua situação internamente e daremos um retorno o mais breve possível.' }, { title: 'Lançamento de Produto', text: 'Chegou o grande dia! Temos o prazer de anunciar o lançamento do nosso novo {{produto_ou_servico}}. Descubra tudo em: {{link_do_produto}}' }, { title: 'Agradecimento de Fim de Ano', text: 'Nesta época festiva, toda a equipa da {{nome_da_empresa}} gostaria de agradecer pela sua confiança e parceria durante este ano. Desejamos-lhe umas Festas Felizes!' } ],
        criadores: [ { title: 'Proposta de Parceria', text: 'Olá, equipa da {{nome_da_marca}}! Sou criador(a) de conteúdo na área de {{nicho}} e um grande admirador do vosso trabalho. Gostaria de apresentar uma proposta de colaboração. O meu media kit está em anexo.' }, { title: 'Agradecimento Pós-Collab', text: 'Foi um prazer colaborar convosco na campanha {{nome_da_campanha}}. O feedback da minha audiência foi fantástico! Espero que possamos trabalhar juntos novamente no futuro.' }, { title: 'Divulgação de Novo Conteúdo', text: 'Olá, pessoal! Acabou de sair vídeo/artigo novo sobre {{tema_do_conteudo}}. Está imperdível! Confiram no link: {{link_do_conteudo}}' }, { title: 'Convite para Live', text: 'Alerta de Live! Na próxima {{dia_da_semana}} às {{hora}}, estarei ao vivo no meu {{plataforma}} para falar sobre {{tema_da_live}} com o convidado especial {{nome_do_convidado}}. Não percam!' }, { title: 'Resposta a Dúvidas (DM)', text: 'Olá! Muito obrigado pela tua mensagem. Essa é uma pergunta excelente e comum! Eu abordei esse tema em detalhe neste vídeo/post: {{link_do_conteudo}}. Espero que ajude!' }, { title: 'Venda de Infoproduto', text: 'Olá! Vi que te interessas por {{tema}}. Abri agora as inscrições para o meu curso/ebook {{nome_do_produto}}, que te vai ensinar a {{beneficio}}. As vagas são limitadas. Sabe mais aqui: {{link_de_venda}}' }, { title: 'Pedido de Feedback', text: 'Olá, {{nome_seguidor}}! A tua opinião é muito importante para mim. O que estás a achar dos últimos conteúdos? Há algum tema que gostarias que eu abordasse?' }, { title: 'Contacto para Imprensa', text: 'Olá, {{nome_jornalista}}. O meu nome é {{meu_nome}} e sou especialista em {{minha_area}}. Vi o seu recente artigo sobre {{tema}} e gostaria de me colocar à disposição para futuros comentários ou entrevistas.' }, { title: 'Cross-Promotion', text: 'E aí, {{nome_colega}}! Admiro muito o teu trabalho. Estava a pensar se não terias interesse em fazermos uma colaboração (uma live, um vídeo em conjunto) para as nossas audiências.' }, { title: 'Divulgação de Afiliação', text: 'Olá! Muitos de vocês perguntam sobre {{produto_que_uso}}. Eu uso e recomendo! Se decidirem comprar através do meu link de afiliado, estarão a apoiar o meu trabalho sem qualquer custo extra para vocês: {{link_afiliado}}' }, { title: 'Contacto a Patrocinadores', text: 'Assunto: Proposta de Parceria para {{nome_da_marca}}. Olá! O meu conteúdo sobre {{nicho}} atinge uma audiência de {{numero_seguidores}}, com um forte engajamento em {{dados_demograficos}}. Acredito que uma parceria seria mutuamente benéfica.' }, { title: 'Aviso de Férias', text: 'Olá, comunidade incrível! Estarei a fazer uma pequena pausa para recarregar energias entre {{data_inicio}} e {{data_fim}}. Voltarei em breve com mais e melhor conteúdo! Obrigado por tudo.' }, { title: 'Pedido de Depoimento (Alunos)', text: 'Olá, {{nome_aluno}}! Fico feliz por teres concluído o meu curso. Se tiveste uma boa experiência, poderias deixar um breve depoimento em vídeo ou texto? Ajudaria imensamente!' }, { title: 'Convite para Comunidade', text: 'Gostas do meu conteúdo? Então vais adorar a minha comunidade privada no {{plataforma}}! Lá partilho dicas exclusivas, bastidores e muito mais. Entra aqui: {{link_comunidade}}' }, { title: 'Lembrete de Evento', text: 'Estamos quase a começar! A nossa live/webinar sobre {{tema}} começa em 15 minutos. Garante o teu lugar e prepara as tuas perguntas! Link de acesso: {{link_do_evento}}' } ]
    };

    // --- INICIALIZAÇÃO DAS BIBLIOTECAS DE TELEFONE ---
    const initIntlTelInput = (inputElement) => {
        return window.intlTelInput(inputElement, {
            utilsScript: "https://cdnjs.cloudflare.com/ajax/libs/intl-tel-input/19.2.16/js/utils.js",
            initialCountry: "br",
            separateDialCode: true,
        });
    };
    itiWhatsapp = initIntlTelInput(numeroInput);
    itiVCard = initIntlTelInput(numeroVCardInput);
    
    // --- FUNÇÕES DE LÓGICA DE VARIÁVEIS E PREVIEW ---
    const parseAdvancedFormatting = (text) => {
        return text
            .replace(/\*(.*?)\*/g, '<b>$1</b>')
            .replace(/_(.*?)_/g, '<i>$1</i>')
            .replace(/~(.*?)~/g, '<s>$1</s>')
            .replace(/```(.*?)```/gs, '<code>$1</code>')
            .replace(/^> (.*$)/gm, '<blockquote>$1</blockquote>');
    };

    const fetchLinkMetadata = async (url) => {
        const proxyUrl = `https://api.allorigins.win/get?url=${encodeURIComponent(url)}`;
        try {
            const response = await fetch(proxyUrl);
            if (!response.ok) return null;
            const data = await response.json();
            const htmlContent = data.contents;
            if (!htmlContent) return null;
            const parser = new DOMParser();
            const doc = parser.parseFromString(htmlContent, 'text/html');
            const getMeta = (prop) => doc.querySelector(`meta[property='${prop}'], meta[name='${prop}']`)?.getAttribute('content') || '';
            return {
                title: getMeta('og:title') || doc.querySelector('title')?.textContent || 'Título não encontrado',
                description: getMeta('og:description') || getMeta('description') || '',
                image: getMeta('og:image') || '',
                siteName: getMeta('og:site_name') || url.split('/')[2]
            };
        } catch (error) {
            console.error("Erro ao buscar metadados do link:", error);
            return null;
        }
    };
    
    const updateLinkPreviewUI = async (text) => {
        const urlRegex = /(https?:\/\/[^\s]+)/;
        const match = text.match(urlRegex);
        if (!match) {
            linkPreviewContainer.classList.add('hidden');
            return;
        }
        const url = match[0];
        linkPreviewContainer.classList.remove('hidden');
        linkPreviewTitle.textContent = 'A carregar pré-visualização...';
        linkPreviewDescription.textContent = '';
        linkPreviewImage.src = '';
        linkPreviewImage.style.display = 'none';
        const metadata = await fetchLinkMetadata(url);
        if (metadata) {
            linkPreviewTitle.textContent = metadata.title;
            linkPreviewDescription.textContent = metadata.description;
            linkPreviewUrl.textContent = metadata.siteName;
            if (metadata.image) {
                linkPreviewImage.src = metadata.image;
                linkPreviewImage.style.display = 'block';
            }
        } else {
            linkPreviewTitle.textContent = url;
            linkPreviewUrl.textContent = url.split('/')[2];
        }
    };
    
    const findVariablesInText = (text) => {
        const regex = /{{\s*([a-zA-Z0-9_]+)\s*}}/g;
        const matches = new Set();
        let match;
        while ((match = regex.exec(text)) !== null) { matches.add(match[1]); }
        return Array.from(matches);
    };

    const renderVariableInputs = (variables) => {
        variableInputsContainer.innerHTML = '';
        if (variables.length === 0) {
            variableInputsContainer.style.display = 'none';
            return;
        }
        variables.forEach(variableName => {
            const node = variableInputTemplate.content.cloneNode(true);
            const label = node.querySelector('.variable-label');
            const input = node.querySelector('.variable-input');
            label.textContent = variableName.replace(/_/g, ' ');
            input.dataset.variable = variableName;
            variableInputsContainer.appendChild(node);
        });
        variableInputsContainer.style.display = 'flex';
    };

    const updateMessageWithVariableValues = () => {
        let newText = masterTemplateText;
        const inputs = variableInputsContainer.querySelectorAll('.variable-input');
        inputs.forEach(input => {
            const variable = input.dataset.variable;
            const value = input.value;
            const placeholder = `{{${variable}}}`;
            const placeholderRegex = new RegExp(placeholder.replace(/([.*+?^=!:${}()|\[\]\/\\])/g, "\\$1"), 'g');
            newText = newText.replace(placeholderRegex, value || placeholder);
        });
        mensagemInput.value = newText;
        updatePreview();
    };

    const processMessageForVariables = () => {
        masterTemplateText = mensagemInput.value;
        const variables = findVariablesInText(masterTemplateText);
        renderVariableInputs(variables);
        updateMessageWithVariableValues();
    };
    
    const updatePreview = () => {
        clearTimeout(linkPreviewDebounce);
        if (currentGeneratorType === 'vcard') {
            const firstName = vcardFirstNameInput.value || '';
            const lastName = vcardLastNameInput.value || '';
            previewContactName.textContent = `${firstName} ${lastName}`.trim() || 'Novo Contato';
        } else {
            const text = mensagemInput.value;
            previewText.innerHTML = parseAdvancedFormatting(text) || "Sua mensagem aparecerá aqui...";
            const now = new Date();
            previewTimestamp.textContent = `${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}`;
            linkPreviewDebounce = setTimeout(() => {
                updateLinkPreviewUI(text);
            }, 500);
        }
    };
    
    const renderTemplates = (category) => {
        templateListDiv.innerHTML = '';
        const templatesToRender = category === 'custom' ? loadFromLocalStorage(CUSTOM_TEMPLATES_KEY) : templates[category];
        if (templatesToRender && templatesToRender.length > 0) {
            templatesToRender.forEach(template => {
                const button = document.createElement('button');
                button.type = 'button';
                button.className = 'template-item';
                button.textContent = template.title;
                button.dataset.text = template.text;
                templateListDiv.appendChild(button);
            });
        } else if (category === 'custom') {
            templateListDiv.innerHTML = '<p class="template-empty-message">Nenhum modelo personalizado salvo.</p>';
        }
    };
    
    const toggleGeneratorUI = (type) => {
        currentGeneratorType = type;
        const isVCard = type === 'vcard';

        whatsappFieldsContainer.classList.toggle('hidden', isVCard);
        vcardFieldsContainer.classList.toggle('hidden', !isVCard);
        
        updatePreview();
        gerarBtnText.textContent = isVCard ? 'Gerar QR Code de Contato' : 'Gerar Link e QR Code';
        
        document.querySelectorAll('.radio-label').forEach(label => {
            const input = label.querySelector('input[name="generatorType"]');
            if (input) {
                label.classList.toggle('active', input.value === type);
            }
        });
        
        if (type === 'whatsapp') {
            renderTemplates(templateCategorySelect.value);
        } else {
            variableInputsContainer.style.display = 'none';
            linkPreviewContainer.classList.add('hidden');
        }
    };

    const handleGeneration = () => {
        let qrCodeData = '';
        const isTrackingChecked = trackClicksCheckbox ? trackClicksCheckbox.checked : false;

        if (currentGeneratorType === 'whatsapp') {
            if (!itiWhatsapp.isValidNumber()) {
                numeroWhatsappErrorSpan.textContent = 'O número de telefone parece inválido.';
                return;
            }
            numeroWhatsappErrorSpan.textContent = '';
            
            const shouldTrack = isTrackingChecked;
            const destinationUrl = `https://wa.me/${itiWhatsapp.getNumber().replace(/\D/g, '')}?text=${encodeURIComponent(mensagemInput.value)}`;

            if (shouldTrack) {
                const linkId = Math.random().toString(36).substr(2, 8);
                const trackableLink = `${window.location.origin}${window.location.pathname}#/track/${linkId}`;
                const linkName = linkNameInput.value.trim() || destinationUrl;
                qrCodeData = trackableLink;
                linkGeradoA.href = trackableLink;
                linkGeradoA.textContent = trackableLink.replace(/^https?:\/\//, '');
                saveTrackedLink({ id: linkId, name: linkName, destinationUrl, shortLink: trackableLink, clicks: 0, createdAt: new Date().toISOString() });
                linkResultGroup.classList.remove('hidden');
            } else {
                qrCodeData = destinationUrl;
                linkGeradoA.href = destinationUrl;
                linkGeradoA.textContent = destinationUrl.replace('https://', '');
                linkResultGroup.classList.remove('hidden');
                saveToHistory({ number: itiWhatsapp.getNumber(), message: mensagemInput.value, timestamp: Date.now() });
            }
        } else if (currentGeneratorType === 'vcard') {
            if (!itiVCard.isValidNumber()) {
                numeroVCardErrorSpan.textContent = 'O número de telefone parece inválido.';
                return;
            }
            numeroVCardErrorSpan.textContent = '';

            const firstName = vcardFirstNameInput.value.trim();
            const lastName = vcardLastNameInput.value.trim();
            if (!firstName || !lastName) {
                alert('Por favor, preencha pelo menos o Primeiro Nome e o Apelido.');
                return;
            }
            const middleName = vcardMiddleNameInput.value.trim();
            const nickname = vcardNicknameInput.value.trim();
            const formattedName = `${firstName} ${middleName} ${lastName}`.replace(/\s+/g, ' ').trim();
            let vCardString = `BEGIN:VCARD\nVERSION:3.0\nN:${lastName};${firstName};${middleName};;\nFN:${formattedName}`;
            if (vcardPhotoBase64) {
                vCardString += `\nPHOTO;ENCODING=b;TYPE=JPEG:${vcardPhotoBase64}`;
            }
            if (nickname) vCardString += `\nNICKNAME:${nickname}`;
            vCardString += `\nTEL;TYPE=CELL:${itiVCard.getNumber()}`;
            if (vcardEmailInput.value) vCardString += `\nEMAIL:${vcardEmailInput.value.trim()}`;
            if (vcardCompanyInput.value) vCardString += `\nORG:${vcardCompanyInput.value.trim()}`;
            if (vcardTitleInput.value) vCardString += `\nTITLE:${vcardTitleInput.value.trim()}`;
            if (vcardWebsiteInput.value) vCardString += `\nURL:${vcardWebsiteInput.value.trim()}`;
            if (vcardLinkedinInput.value) vCardString += `\nURL;type=LinkedIn:${vcardLinkedinInput.value.trim()}`;
            if (vcardInstagramInput.value) vCardString += `\nX-SOCIALPROFILE;type=instagram:https://instagram.com/${vcardInstagramInput.value.replace('@', '').trim()}`;
            if (vcardTwitterInput.value) vCardString += `\nX-SOCIALPROFILE;type=twitter:https://twitter.com/${vcardTwitterInput.value.replace('@', '').trim()}`;
            if (vcardGithubInput.value) vCardString += `\nX-SOCIALPROFILE;type=github:https://github.com/${vcardGithubInput.value.trim()}`;
            if (vcardTelegramInput.value) vCardString += `\nX-SOCIALPROFILE;type=telegram:https://t.me/${vcardTelegramInput.value.replace('@', '').trim()}`;
            if (vcardYoutubeInput.value) vCardString += `\nURL;type=YouTube:${vcardYoutubeInput.value.trim()}`;
            if (vcardRedditInput.value) vCardString += `\nX-SOCIALPROFILE;type=reddit:https://www.reddit.com/user/${vcardRedditInput.value.replace('u/', '').trim()}`;
            if (vcardAddressInput.value || vcardCityInput.value) {
                vCardString += `\nADR;TYPE=HOME:;;${vcardAddressInput.value.trim()};${vcardCityInput.value.trim()};;;`;
            }
            if (vcardNotesInput.value) vCardString += `\nNOTE:${vcardNotesInput.value.replace(/\n/g, '\\n')}`;
            vCardString += `\nEND:VCARD`;
            qrCodeData = vCardString;
            linkResultGroup.classList.add('hidden');
        }

        resultArea.style.display = 'flex';
        qrCodeContainer.innerHTML = '';
        const isDarkMode = body.getAttribute('data-theme') === 'dark';
        qrDotColorInput.value = isDarkMode ? "#EAEAEA" : "#111B21";
        qrBgColorInput.value = isDarkMode ? "#1E1E1E" : "#FFFFFF";
        qrCodeInstance = new QRCodeStyling({ width: 200, height: 200, data: qrCodeData, margin: 0, dotsOptions: { color: qrDotColorInput.value, type: "rounded" }, backgroundOptions: { color: qrBgColorInput.value }, cornersSquareOptions: { type: "extra-rounded" }, cornersDotOptions: { type: "dot" } });
        qrCodeInstance.append(qrCodeContainer);
    };
    
    const loadFromLocalStorage = (key) => JSON.parse(localStorage.getItem(key)) || [];
    const saveToLocalStorage = (key, data) => localStorage.setItem(key, JSON.stringify(data));
    const saveToHistory = (newItem) => { let h = loadFromLocalStorage(HISTORY_KEY); h.unshift(newItem); if (h.length > MAX_HISTORY_ITEMS) h.pop(); saveToLocalStorage(HISTORY_KEY, h); loadHistory(); };
    const saveTrackedLink = (newLink) => { const l = loadFromLocalStorage(ANALYTICS_KEY); l.unshift(newLink); saveToLocalStorage(ANALYTICS_KEY, l); loadAnalytics(); };
    const loadAllData = () => { loadHistory(); loadCustomTemplates(); loadAnalytics(); };
    const loadHistory = () => {
        historyListContainer.innerHTML = '';
        const history = loadFromLocalStorage(HISTORY_KEY);
        if (history.length === 0) { historyListContainer.innerHTML = '<p class="history-empty-message">Nenhum link foi gerado ainda.</p>'; return; }
        history.forEach(item => { const n = historyItemTemplate.content.cloneNode(true), c = n.querySelector('.history-copy-btn'); n.querySelector('.history-item-number').textContent = item.number; n.querySelector('.history-item-message').textContent = item.message || '(Sem mensagem)'; n.querySelector('.history-reuse-btn').addEventListener('click', () => { document.querySelector('input[name="generatorType"][value="whatsapp"]').click(); itiWhatsapp.setNumber(item.number); mensagemInput.value = item.message; processMessageForVariables(); window.scrollTo({ top: 0, behavior: 'smooth' }); }); c.addEventListener('click', () => copiarTexto(`https://wa.me/${item.number.replace(/\D/g, '')}?text=${encodeURIComponent(item.message)}`, c)); n.querySelector('.history-delete-btn').addEventListener('click', () => { saveToLocalStorage(HISTORY_KEY, history.filter(h => h.timestamp !== item.timestamp)); loadHistory(); }); historyListContainer.appendChild(n); });
    };
    const loadCustomTemplates = () => {
        customTemplateListContainer.innerHTML = '';
        const t = loadFromLocalStorage(CUSTOM_TEMPLATES_KEY); if (t.length === 0) { customTemplateListContainer.innerHTML = '<p class="history-empty-message">Você ainda não salvou nenhum modelo.</p>'; return; }
        t.forEach(tm => { const n = customTemplateItemTemplate.content.cloneNode(true); n.querySelector('.custom-template-item-title').textContent = tm.title; n.querySelector('.custom-template-item-text').textContent = tm.text; n.querySelector('.custom-template-use-btn').addEventListener('click', () => { document.querySelector('input[name="generatorType"][value="whatsapp"]').click(); mensagemInput.value = tm.text; processMessageForVariables(); window.scrollTo({ top: 0, behavior: 'smooth' }); }); n.querySelector('.custom-template-delete-btn').addEventListener('click', () => { if (confirm(`Tem certeza que deseja apagar o modelo "${tm.title}"?`)) { saveToLocalStorage(CUSTOM_TEMPLATES_KEY, t.filter(i => i.id !== tm.id)); loadCustomTemplates(); renderTemplates(templateCategorySelect.value); } }); customTemplateListContainer.appendChild(n); });
    };
    const loadAnalytics = () => {
        analyticsListContainer.innerHTML = '';
        const l = loadFromLocalStorage(ANALYTICS_KEY); if (l.length === 0) { analyticsListContainer.innerHTML = '<p class="history-empty-message">Você ainda não criou nenhum link rastreável.</p>'; return; }
        l.forEach(lk => { const n = analyticsItemTemplate.content.cloneNode(true), s = n.querySelector('.analytics-item-shortlink'), c = n.querySelector('.copy-analytics-link-btn'); n.querySelector('.analytics-item-name').textContent = lk.name; n.querySelector('.analytics-item-destination').textContent = lk.destinationUrl; s.href = lk.shortLink; s.textContent = lk.shortLink; n.querySelector('.click-count').textContent = lk.clicks; c.addEventListener('click', () => copiarTexto(lk.shortLink, c)); n.querySelector('.delete-analytics-link-btn').addEventListener('click', () => { if (confirm(`Tem certeza que deseja apagar o registro deste link?\n${lk.shortLink}`)) { saveToLocalStorage(ANALYTICS_KEY, l.filter(i => i.id !== lk.id)); loadAnalytics(); } }); analyticsListContainer.appendChild(n); });
    };
    const handleExport = () => { const d = { history: loadFromLocalStorage(HISTORY_KEY), customTemplates: loadFromLocalStorage(CUSTOM_TEMPLATES_KEY), analytics: loadFromLocalStorage(ANALYTICS_KEY) }; const j = JSON.stringify(d, null, 2), b = new Blob([j], { type: 'application/json' }), u = URL.createObjectURL(b), a = document.createElement('a'); a.href = u; const ts = new Date().toISOString().slice(0, 10); a.download = `gwbrasil_backup_${ts}.json`; document.body.appendChild(a); a.click(); document.body.removeChild(a); URL.revokeObjectURL(u); };
    const handleImport = (e) => {
        const f = e.target.files[0]; if (!f) return; if (f.type !== 'application/json') { alert('Por favor, selecione um ficheiro .json válido.'); return; }
        const r = new FileReader(); r.onload = (ev) => { try { const d = JSON.parse(ev.target.result); if (typeof d.history !== 'object' || typeof d.customTemplates !== 'object' || typeof d.analytics !== 'object') { throw new Error('Formato de dados inválido.'); } if (confirm('Atenção: Isto irá substituir todos os seus dados atuais (histórico, modelos e analytics) pelos dados do ficheiro. Deseja continuar?')) { saveToLocalStorage(HISTORY_KEY, d.history || []); saveToLocalStorage(CUSTOM_TEMPLATES_KEY, d.customTemplates || []); saveToLocalStorage(ANALYTICS_KEY, d.analytics || []); loadAllData(); renderTemplates(templateCategorySelect.value); alert('Dados importados com sucesso!'); } } catch (er) { alert(`Erro ao ler o ficheiro: ${er.message}`); } finally { importDataInput.value = ''; } }; r.readAsText(f);
    };

    // --- EVENT LISTENERS ---
    linkForm.addEventListener('submit', e => { e.preventDefault(); handleGeneration(); });
    generatorTypeRadios.forEach(radio => radio.addEventListener('change', e => toggleGeneratorUI(e.target.value)));
    mensagemInput.addEventListener('input', () => { processMessageForVariables(); updatePreview(); });
    [vcardFirstNameInput, vcardLastNameInput, vcardMiddleNameInput].forEach(el => el.addEventListener('input', updatePreview));
    if (trackClicksCheckbox) trackClicksCheckbox.addEventListener('change', (e) => linkNameGroup.classList.toggle('hidden', !e.target.checked));
    templateCategorySelect.addEventListener('change', e => renderTemplates(e.target.value));
    themeToggle.addEventListener('click', () => { const n = body.getAttribute('data-theme') === 'dark' ? 'light' : 'dark'; body.setAttribute('data-theme', n); localStorage.setItem('theme', n); });
    limparBtn.addEventListener('click', () => { linkForm.reset(); resultArea.style.display = 'none'; numeroWhatsappErrorSpan.textContent = ''; numeroVCardErrorSpan.textContent = ''; itiWhatsapp.setNumber(''); itiVCard.setNumber(''); vcardPhotoPreview.classList.add('hidden'); vcardPhotoBase64 = ''; document.querySelector('input[name="generatorType"][value="whatsapp"]').click(); toggleGeneratorUI('whatsapp'); updatePreview(); variableInputsContainer.style.display = 'none'; masterTemplateText = ''; });
    abrirWppBtn.addEventListener('click', () => { if (itiWhatsapp.isValidNumber()) { window.open(`https://wa.me/${itiWhatsapp.getNumber().replace(/\D/g, '')}?text=${encodeURIComponent(mensagemInput.value)}`, '_blank'); } else { numeroWhatsappErrorSpan.textContent = 'Insira um número válido para abrir no WhatsApp.'; } });
    copiarBtn.addEventListener('click', () => copiarTexto(linkGeradoA.href, copiarBtn));
    templateListDiv.addEventListener('click', e => { if (e.target.classList.contains('template-item')) { mensagemInput.value = e.target.dataset.text; processMessageForVariables(); updatePreview(); mensagemInput.focus(); } });
    customTemplateForm.addEventListener('submit', e => { e.preventDefault(); const t = customTemplateTitleInput.value.trim(), x = customTemplateTextInput.value.trim(); if (t && x) { const n = { id: Date.now(), title: t, text: x }, m = loadFromLocalStorage(CUSTOM_TEMPLATES_KEY); m.unshift(n); saveToLocalStorage(CUSTOM_TEMPLATES_KEY, m); loadCustomTemplates(); renderTemplates(templateCategorySelect.value); customTemplateForm.reset(); } });
    qrDownloadBtn.addEventListener('click', () => qrCodeInstance?.download({ name: "qrcode-geradorwhats", extension: qrDownloadFormat.value }));
    qrLogoUpload.addEventListener('change', e => { if (!qrCodeInstance || !e.target.files || !e.target.files[0]) return; const r = new FileReader(); r.onload = (ev) => qrCodeInstance.update({ image: ev.target.result }); r.readAsDataURL(e.target.files[0]); });
    vcardPhotoUpload.addEventListener('change', (e) => {
        const file = e.target.files[0];
        if (!file) return;
        const reader = new FileReader();
        reader.onload = (event) => {
            vcardPhotoPreview.src = event.target.result;
            vcardPhotoPreview.classList.remove('hidden');
            vcardPhotoBase64 = event.target.result.substring(event.target.result.indexOf(',') + 1);
        };
        reader.readAsDataURL(file);
    });
    qrDotColorInput.addEventListener('input', e => qrCodeInstance?.update({ dotsOptions: { color: e.target.value } }));
    qrBgColorInput.addEventListener('input', e => qrCodeInstance?.update({ backgroundOptions: { color: e.target.value } }));
    [formatBoldBtn, formatItalicBtn, formatStrikeBtn].forEach(btn => { btn.addEventListener('click', () => { const c = btn.id === 'format-bold' ? '*' : (btn.id === 'format-italic' ? '_' : '~'), s = mensagemInput.selectionStart, e = mensagemInput.selectionEnd; if (e > s) { const t = mensagemInput.value; mensagemInput.value = `${t.substring(0, s)}${c}${t.substring(s, e)}${c}${t.substring(e)}`; updatePreview(); processMessageForVariables(); mensagemInput.focus(); mensagemInput.setSelectionRange(s + c.length, e + c.length); } }); });
    exportDataBtn.addEventListener('click', handleExport);
    importDataInput.addEventListener('change', handleImport);
    variableInputsContainer.addEventListener('input', e => { if (e.target.classList.contains('variable-input')) { updateMessageWithVariableValues(); } });

    // --- INICIALIZAÇÃO DA PÁGINA ---
    const initializePage = () => {
        body.setAttribute('data-theme', localStorage.getItem('theme') || (window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'));
        loadAllData();
        toggleGeneratorUI('whatsapp');
        updatePreview();
    };

    initializePage();
});