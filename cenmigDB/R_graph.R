#!/usr/bin/env Rscript
args = commandArgs(trailingOnly=TRUE)

library(plotly)
library(ggplot2)
library(dplyr)
library(tidyr)
library(maps)
library(viridis)
library(scales)
library(ggrepel)
library(stringr)
library(cowplot)
library(rworldmap)
Sys.setenv("DISPLAY"=":0.0")
options(bitmapType='cairo') ## Add this to run with unix

file_out = args

print('Loading CSV file')

meta = read.csv(paste0(file_out, 'metadata_R_tmp.csv', sep = ''),na.strings=c("NA","NaN", "", " "))
resist = read.csv(paste0(file_out, 'resist_R_tmp.csv', sep = ''),na.strings=c("NA","NaN", "", " "))
species = scan(paste0(file_out, 'spp_R_tmp.txt', sep = ''), character(), sep=',')

print('Loading Completed')

# Function
loop_run = function(meta, resist ,species, file_out){
  for (spp in species) {
    print(spp)
    lineage_grouping(meta,spp, graph_name = 'lineage_grouping')
    genome_overtime(meta,spp, graph_name = 'Genome_over_time')
    genome_by_country(meta,spp, graph_name = 'Genome_by_country')
    country_heatmap(meta, spp, file_out, graph_name = 'Genome_by_country_heatmap')
    age_count_human(meta, spp, file_out, graph_name = 'Age_grouping')
    sex_ratio_by_time(meta, spp, file_out, graph_name = 'sex_ratio')   
    phenotype_resistant(meta, resist, spp, file_out, graph_name = 'Phenotype_resistant')
    phenotype_resistant_by_ST(meta, resist, spp, file_out, file_name = 'Phenotype_resistant_by_ST')
  }
}

lineage_grouping = function(meta,spp, graph_name){
  data = query = paste(spp, '*', sep='')
  data <- meta %>% filter(grepl(query, Organism))
  
  data = data.frame(data$cenmigID, data$ST)
  colnames(data) = c('cenmigID', 'ST')
  
  data$Type[!is.na(data$ST)] <- 'Data available'  
  data$Type[is.na(data$ST)] <- 'Data not available' 
  
  # Filter only 80 % Cumulative
  data_ST = data %>% drop_na(ST) %>% group_by(ST) %>% summarise(count = n())
  data_ST = data_ST %>% arrange(desc(count)) %>% mutate(csum = cumsum(count))
  ## Get 80% of data
  limit_num = round(max(data_ST$csum) * 0.8)
  filter_major = data_ST[data_ST$csum <= limit_num,]
  
  ### Assign other if not in major
  data_ST$Group = data_ST$ST
  data_ST$Group[!data_ST$ST %in% filter_major$ST] <- 'Other'
  
  
  data_plot = merge(data, data_ST, by = 'ST', all.x = TRUE)
  data_plot$Group <- sub("^0$", "Unassigned", data_plot$Group, ignore.case = TRUE)
  # Plot
  plot = ggplot(data_plot %>% drop_na(ST) %>% arrange(desc(count)) ,
            aes(x=reorder(Group,-count))) +
            ggtitle('MLST sequence typing') +
            geom_bar(stat="count",alpha = 0.7, fill= '#0099CC') +
            theme(axis.text.x = element_text(angle = 90, hjust = 1, vjust = 0.5),
                  legend.position = 'none',
                  plot.title = element_text(hjust = 0.5)) +
            ylab("Number of genome sequences") + 
            xlab('Sequence typing')
    
  plot = plot + guides(fill=guide_legend(nrow=5, byrow=TRUE,override.aes = list(size = 0.1),
                                          title = "ST"))
  plot = plot + guides(color = guide_legend(override.aes = list(size = 0.01)))
  #plot
  
  ## Pie 
  data_type = data %>% group_by(Type) %>% summarise(count = n())
  data_type = data_type %>%
    arrange(desc(count)) %>%
    mutate(prop = percent(count / sum(count)))
  
  pie = ggplot(data_type, aes(x='', y=count, fill=Type)) +
    geom_bar(stat="identity", width=1) +
    theme_void() +
    coord_polar("y", start= 0) +
    geom_label_repel(aes(label = prop), size=4.5, show.legend = F, nudge_x = 1) +
    guides(fill = guide_legend(title = "Data Type"))
  
  #pie 
  # Overlay graph
  overlay.plot = ggdraw() +
    draw_plot(plot) 
    # + draw_plot(pie, x = .5, y = .6, width = .4, height = .4)
  #overlay.plot
  
  # Export
  name = str_replace(spp, ' ', '_')
  file_csv = paste(file_out, name, '_', graph_name, '.csv', sep='')
  write.csv(data_ST, file_csv, row.names = FALSE)
  file_plot = paste(file_out, name, '_', graph_name, '.png', sep='')
  ggsave( file_plot, overlay.plot, dpi = 300)

}

genome_overtime = function(meta,spp, graph_name){
  query = paste(spp, '*', sep='')
  data <- meta %>% filter(grepl(query, Organism))
  
  #Remove NA
  sra <- data[!is.na(data$Run),]
  sra$Type = 'SRA'
  fasta <- data[!is.na(data$asm_acc),]
  fasta$Type = 'FASTA'
  
  
  # Plot Bar
  sra = data.frame(sra$Year,sra$Type)
  fasta = data.frame(fasta$Year,fasta$Type)
  
  colnames(sra) <- c('Year','Type')
  colnames(fasta) <- c('Year','Type')
  
  data_plot = rbind(sra,fasta)
  now_year = format(Sys.Date(), '%Y')
  now_year = as.integer(now_year)
  
  
  plot = (ggplot(data_plot ,aes(x = Year, fill = Type)) +
          geom_bar(stat="count",alpha = 0.7) +
          theme(axis.text.x = element_text(angle = 90)) + 
          ylab("Number of genome sequences") +
          #theme(legend.position = c(0.15, 0.8)) +
          theme(legend.position = 'bottom') +
          scale_x_continuous(breaks = seq(1990, now_year,5), limits = c(1990, now_year) ) 
          #scale_fill_manual(values = c("#FF0000", "#0089FF")) +
          #scale_color_manual(values = c("#FF0000", "#0089FF"))
          
          )
  #plot
  # Plot Pie miss date data
  data_date = data
  data_date$Type = 'NA'
  data_date$Type[!is.na(data_date$Year)] <- 'Date available'  
  data_date$Type[is.na(data_date$Year)] <- 'Date not available' 
  data_date = data.frame(data_date$Year,data_date$Type)
  colnames(data_date) <- c('Year','Type')
  data_date = data_date %>% group_by(Type) %>% summarise(count = n())
  
  data_date = data_date %>%
              arrange(desc(count)) %>%
              mutate(prop = percent(count / sum(count)))
  
  pie = ggplot(data_date, aes(x='', y=count, fill=Type)) +
        geom_bar(stat="identity", width=1) +
        theme_void() +
        coord_polar("y", start= 0) +
        geom_label_repel(aes(label = prop), size=4.5, show.legend = F, nudge_x = 1) +
        guides(fill = guide_legend(title = "Data Type"))
  
  
  #pie 
  
  ## Merge two graph
  main.plot <- plot_grid(plot,
                         pie, 
                         align = "h", rel_widths =c(2, 1))
  #main.plot
  
  overlay.plot = ggdraw() +
              draw_plot(plot) +
              draw_plot(pie, x = .1, y = .5, width = .5, height = .5)
  #overlay.plot
  
  
  # Export DF
  data_out = data_plot %>% group_by(Year) %>% summarise(count = n())
  
  name = str_replace(spp, ' ', '_')
  file_csv = paste(file_out, name, '_', graph_name, '.csv', sep='')
  write.csv(data_out, file_csv, row.names = FALSE)
  file_plot = paste(file_out, name, '_', graph_name, '.png', sep='')
  ggsave( file_plot, overlay.plot, dpi = 300)
      
}

genome_by_country = function(meta,spp, graph_name){
  query = paste(spp, '*', sep='')
  data <- meta %>% filter(grepl(query, Organism))
  
  country = data.frame(data$geo_loc_name_country_fix,data$cenmigID)
  colnames(country) <- c('Country','cenmigID')
  
  # Clean dirty data
  pattern_fil_out = c('uncalculated','not.collect.*','hospital*')
  pattern_fil_out = paste(pattern_fil_out, collapse="|")
  country <- country %>% filter(!grepl(pattern_fil_out, Country))
  #country$Country <- country$Country %>% replace_na('No data')
  data_out = country %>% group_by(Country) %>% summarise(count = n())
  country_low = data_out$Country[!data_out$count <= 100]
  country = country[which(country$Country %in% country_low),]
  
  plot = (
    ggplot(country, aes(Country, fill = Country)) +
      geom_bar(stat="count") +
      # scale_fill_manual(values = ["#00AFBC"]) +
      # scale_color_manual(values = ["#00AFBB"]) +
      # scale_x_discrete(limits= list(data['Country'].unique())) +
      scale_y_log10() +
      theme(axis.title.x = element_blank(),
            axis.text.x = element_text(face="bold", size=6, angle=90, hjust=1,vjust=0.2),
            axis.ticks.x = element_blank(),
            legend.position = 'none'))
            
            #legend.position = "bottom",
            #legend.text = element_text(size = 4.5)))
  #plot = plot + guides(fill=guide_legend(nrow=12, byrow=TRUE,override.aes = list(size = 0.1)))
  #plot = plot + guides(color = guide_legend(override.aes = list(size = 0.01)))
  #plot                     
  
  # Export
  name = str_replace(spp, ' ', '_')
  file_csv = paste(file_out, name, '_', graph_name, '.csv', sep='')
  write.csv(data_out, file_csv, row.names = FALSE)
  file_plot = paste(file_out, name, '_', graph_name, '.png', sep='')
  ggsave( file_plot, plot, dpi = 300)
  
}

country_heatmap = function(meta, spp, file_out, graph_name, ungeo_df = ungeo){
  query = paste(spp, '*', sep='')
  data <- meta %>% filter(grepl(query, Organism))
  
  country = data.frame(data$geo_loc_name_country_fix,data$cenmigID, data$ISO.alpha3.Code)
  colnames(country) <- c('Country','cenmigID', 'ISO3')
  
  # Clean dirty data
  pattern_fil_out = c('uncalculated','not.collect.*','hospital*')
  pattern_fil_out = paste(pattern_fil_out, collapse="|")
  country <- country %>% filter(!grepl(pattern_fil_out, Country))
  #country$Country <- country$Country %>% replace_na('No data')
  data_plot = country %>% group_by(ISO3) %>% summarise(count = n())
  colnames(data_plot) <- c('ISO3', 'count')
  data_plot$count = log(data_plot$count)
  
  ## Plot heat map
  # Match to map with iso3
  name = str_replace(spp, ' ', '_')
  file_plot = paste(file_out, name, '_', graph_name, '.png', sep='')
  png(file_plot, width=4800,height=3200,units="px", res = 600)
  
  
  plot_df = joinCountryData2Map(data_plot, joinCode="NAME", nameJoinColumn="ISO3")
  mapParams = mapCountryData(plot_df, nameColumnToPlot="count", catMethod="fixedWidth",
                 colourPalette = c( "white", "pink", "red1"),
                 missingCountryCol = gray(.8),
                 oceanCol = "azure2",
                 mapTitle = 'Genome by Country',
                 mapRegion = 'world')
  text(0, -133,"Log10(Count)",cex = 1.2)
  dev.off()
  #dev.print(png, file = file_plot, width = 4800, height = 3200, res = 600)

}

age_count_human= function(meta, spp, file_out, graph_name){
  query = paste(spp, '*', sep='')
  data <- meta %>% filter(grepl(query, Organism))
  
  age = data.frame(data$host,data$host_age)
  colnames(age) <- c('host','host_age')
  
  # Clean dirty data
  pattern_fil_in = c('Homo.sapien.*','human.*','homo.*')
  pattern_fil_in = paste(pattern_fil_in, collapse="|")
  age <- age %>% filter(grepl(pattern_fil_in, host),ignore.case=TRUE)
  age <- age %>% filter(!grepl('non.*', host,ignore.case=TRUE))
  age$host_age <- sub("Y.*$", "", age$host_age, ignore.case = TRUE)
  
  # Data which interval get low end
  age$host_age <- gsub("-\\d.*$", "", age$host_age)
  age$host_age <- as.numeric(age$host_age)
  
  data_plot <- age %>% mutate(age_range = ifelse(host_age < 15, "<15",
                                            ifelse(host_age %in% 16:25,"16-25",
                                            ifelse(host_age %in% 26:35,"26-35",
                                            ifelse(host_age %in% 36:45,"36-45",
                                            ifelse(host_age %in% 46:55,"46-55",">55"))))))

  data_plot$age_range <- data_plot$age_range %>% replace_na('No data')
  data_plot <- data_plot %>% mutate(Data_type = ifelse(age_range != 'No data', 
                                                                 "Data available", 
                                                                 'Date not available' ))
  
  plot = ggplot(data_plot %>% drop_na(age_range), aes(age_range, fill = age_range)) +
         geom_bar(stat="count", alpha = 1) +
         #scale_fill_manual(values = c('#00B8E5', '#00C0B8', '#00C19C', '#00BC59', '#00B442', '#00A01F')) +
         scale_x_discrete(limits=c("<15", "16-25","26-35", "36-45", "46-55", ">55")) +
        theme(legend.position = 'bottom')
  
  #plot
  # Export DF
  data_out = data_plot %>% group_by(age_range) %>% summarise(count = n())
  
  # Plot Pie 
  data_plot = data_plot %>% group_by(Data_type) %>% summarise(count_data_type = n())
  data_plot = data_plot %>%
              arrange(desc(count_data_type)) %>%
              mutate(prop = percent(count_data_type / sum(count_data_type)))
  
  pie = ggplot(data_plot, aes(x='', y=count_data_type, fill=Data_type)) +
              geom_col(width = 1, color = 1) +
              geom_bar(stat="identity", width=1) +
              theme_void() +
              coord_polar("y") +
              geom_label_repel(aes(y = count_data_type, label = prop), 
                               size=4.5, show.legend = F, nudge_x = 2) +
              guides(fill = guide_legend(title = "Data Type")) +
              scale_fill_manual(values = c("#00C0BB","#FF689F"))
      
  #pie
  
  # Merge plot
  main.plot <- plot_grid(plot,
                         pie, 
                         align = "h", rel_widths =c(2, 1.2))
  #main.plot
  
  #main.plot = ggdraw() +
  #  draw_plot(plot) +
  # draw_plot(pie, x = .5, y = .7, width = .6, height = .3)
  #main.plot
  
  # Export
  name = str_replace(spp, ' ', '_')
  file_csv = paste(file_out, name, '_', graph_name, '.csv', sep='')
  write.csv(data_out, file_csv, row.names = FALSE)
  file_plot = paste(file_out, name, '_', graph_name, '.png', sep='')
  ggsave( file_plot, main.plot, dpi = 300)
  
}

sex_ratio_by_time = function(meta, spp, file_out, graph_name ){
  query = paste(spp, '*', sep='')
  data <- meta %>% filter(grepl(query, Organism))
  
  ## Select Human
  sex = data.frame(data$host,data$Year,data$host_sex)
  colnames(sex) <- c('host','Year','host_sex')
  
  # Clean dirty data
  pattern_fil_in = c('Homo.sapien.*','human.*','homo.*')
  pattern_fil_in = paste(pattern_fil_in, collapse="|")
  sex <- sex %>% filter(grepl(pattern_fil_in, host),ignore.case=TRUE)
  
  now_year = format(Sys.Date(), '%Y')
  now_year = as.integer(now_year)
  
  # Assign sex
  sex$Gender = NA
  sex$Gender <-  ifelse(grepl('^male.*', sex$host_sex), 'M',
                ifelse(grepl('^female.*', sex$host_sex), 'F', NA))
  
  sex$Data_type <- ifelse(grepl('M', sex$Gender), 'Data available',
                     ifelse(grepl('F', sex$Gender), 'Data available', 'Data Not available'))
  
  data_out = sex %>% group_by(Year) %>% summarise(count = n())
  
  ## fill
  fig_1 = ggplot(sex %>% drop_na(Gender),aes(x = Year, fill = Gender)) +
    stat_bin(color = NA,binwidth =1, alpha = 1, position = "fill") +
    #scale_x_date(breaks = date_breaks("1 month"), labels = date_format("%Y-%m")) +
    ylab("Proportion") +
    theme_classic() +
    scale_x_continuous(breaks = seq(1990, now_year,5), limits = c(1990, now_year) ) +
    theme(axis.text.x = element_text(angle = 90),
          legend.position="bottom") +
    #scale_fill_viridis_d() +
    #scale_fill_brewer(palette = "Set1") +
    #scale_fill_discrete(labels = c("Alpha", "Beta", "Delta", "B.1.16.36(Samut sakorn)", "Serine-Whuhan", "Other")) +
    labs(fill = "Gender")
  #fig_1
  ## Stack
  fig_2 = ggplot(sex %>% drop_na(Gender),aes(x = Year, fill = Gender)) +
    stat_bin(color = NA,binwidth =1, alpha = 1, position = "stack") +
    #scale_x_date(breaks = date_breaks("1 month"), labels = date_format("%Y-%m")) +
    ylab("Count") +
    theme_classic() +
    scale_x_continuous(breaks = seq(1990, now_year,5), limits = c(1990, now_year) ) +
    theme(axis.text.x = element_text(angle = 90),
          legend.position="bottom") +
    labs(fill = "Gender")
  #fig_2
  ## Pie chart
  sex = sex %>% group_by(Data_type) %>% summarise(count_data_type = n())
  sex = sex %>%
    arrange(desc(count_data_type)) %>%
    mutate(prop = percent(count_data_type / sum(count_data_type)))
  
  fig_3 = ggplot(sex, aes(x="", y=count_data_type, fill=Data_type)) +
    geom_bar(stat="identity", width=1) +
    theme_void() +
    coord_polar("y", start= 0) +
    geom_label_repel(aes(label = prop), size=5, show.legend = F) +
    guides(fill = guide_legend(title = "Data Type"))
  #fig_3
  
  
  ### Merge 3 graph
  main.plot <- plot_grid(fig_2 + theme(legend.position="none",
                                      axis.title.x = element_blank(),
                                      axis.text.x = element_blank()),
                         fig_1 + theme(legend.position="none"), 
                         ncol = 1, nrow = 2, align = "h", rel_heights = c(1, 1, 2))
  #main.plot
  legend <- get_legend(
  # create some space to the left of the legend
  fig_1 + theme(legend.position = 'right'))
  
  main.plot = plot_grid(main.plot, legend, rel_widths = c(5, .4))
  
  overlay.plot = ggdraw() +
            draw_plot(main.plot) +
            draw_plot(fig_3, x = .1, y = .7, width = .5, height = .3)
    
  #overlay.plot
  
  # Export
  name = str_replace(spp, ' ', '_')
  file_csv = paste(file_out, name, '_', graph_name, '.csv', sep='')
  write.csv(data_out, file_csv, row.names = FALSE)
  file_plot = paste(file_out, name, '_', graph_name, '.png', sep='')
  ggsave( file_plot, overlay.plot, dpi = 300)
  
  }

phenotype_resistant = function(meta, resist, spp, file_out, graph_name){
  query = paste(spp, '*', sep='')
  data <- meta %>% filter(grepl(query, Organism))
  
  data_resist = resist[resist$cenmigID %in% data$cenmigID,]
  
  # Drop duplicated (Extract only non duplicated phenotype)
  data_resist = distinct(data_resist, cenmigID, Phenotype, keep.all = TRUE)
  
  # Count all records
  total = meta[meta$sub_region == 'South-eastern Asia',]
  total = length(unique(total$cenmigID))
  
  drug_phenotype = data.frame(resist$cenmigID,resist$Phenotype,resist$Class,resist$Resistance.gene)
  colnames(drug_phenotype) = c('cenmigID', 'Phenotype', 'Class', 'Resistance gene')
  drug_phenotype = drug_phenotype[!duplicated(drug_phenotype[, c('cenmigID', 'Phenotype')], fromLast=F),]
  ## Edit some data
  drug_phenotype$Phenotype <- sub('.Alter.*', '', drug_phenotype$Phenotype, ignore.case = TRUE)
  drug_phenotype$Phenotype <- sub('.Amino acid sequence.*', '', drug_phenotype$Phenotype, ignore.case = TRUE)
  drug_phenotype$Phenotype <- sub('Warning.*', 'No phenotype information available', drug_phenotype$Phenotype, ignore.case = TRUE)
  drug_phenotype$Phenotype <- str_c(drug_phenotype$Class, ' : ', drug_phenotype$Phenotype)
  drug_phenotype = drug_phenotype %>% drop_na(Phenotype)
  
  # Extract gene based on Phenotype
  df_pheno_gene = data.frame()
  for (pheno in unique(drug_phenotype$Phenotype)) {
    #print(pheno)
    df_i = drug_phenotype[drug_phenotype$Phenotype == pheno,]
    gene = unique(df_i$`Resistance gene`)
    gene = as.vector(gene)
    df_j = data.frame(pheno,paste(gene,collapse=", "))
    df_pheno_gene = rbind(df_pheno_gene, df_j)
  }
  colnames(df_pheno_gene) = c('Phenotype', 'Gene list')
  
  # Count
  data_out = drug_phenotype %>% group_by(Phenotype) %>% summarise(count = n())
  data_out$Percent = data_out$count *100 / total
  data_out = merge(x = data_out, y = df_pheno_gene, by = "Phenotype", all.x = TRUE)
  filter_out = data_out[data_out$Percent >= 1,]
  
  data_plot = drug_phenotype[drug_phenotype$Phenotype %in% filter_out$Phenotype,]
  data_plot$Phenotype <- sub(' : ', ':\n', data_plot$Phenotype, ignore.case = TRUE)
  # Plot
  
  plot = ggplot(data_plot, aes(Phenotype, fill = Phenotype)) +
    geom_bar(stat="count",alpha = 0.7) +
    # scale_fill_manual(values = ["#00AFBC"]) +
    # scale_color_manual(values = ["#00AFBB"]) +
    # scale_x_discrete(limits= list(data_plot['Phenotype'].unique()))
    theme(axis.title.x = element_blank(),
          axis.text.x = element_text(angle = 90, hjust = 0.9, vjust = 0.5),
          axis.ticks = element_blank(),
          legend.position = "none",
          legend.text = element_text(size = 5)) +
    labs(fill = "Phenotype")
  
  plot = plot + guides(fill=guide_legend(nrow=5, byrow=TRUE,override.aes = list(size = 0.3)))
  #plot = plot + guides(color = guide_legend(override.aes = list(size = 0.3)))
  #plot
  
  # Export
  name = str_replace(spp, ' ', '_')
  file_csv = paste(file_out, name, '_', graph_name, '.csv', sep='')
  write.csv(data_out, file_csv, row.names = FALSE)
  file_plot = paste(file_out, name, '_', graph_name, '.png', sep='')
  ggsave( file_plot, plot, dpi = 300)
  
}

phenotype_resistant_by_ST = function(meta, resist, spp, file_out, file_name){
  query = paste(spp, '*', sep='')
  data <- meta %>% filter(grepl(query, Organism))
  
  drug_phenotype = resist[resist$cenmigID %in% data$cenmigID, ]
  
  # Count all records
  total = data[data$sub_region == 'South-eastern Asia',]
  total = length(unique(total$cenmigID))
  
  drug_phenotype = data.frame(drug_phenotype$cenmigID,drug_phenotype$Phenotype,
                              drug_phenotype$Class,drug_phenotype$Resistance.gene)
  colnames(drug_phenotype) = c('cenmigID', 'Phenotype', 'Class', 'Resistance gene')
  drug_phenotype = drug_phenotype[!duplicated(drug_phenotype[c('cenmigID', 'Phenotype')]),]
  
  ## Edit some data
  drug_phenotype$Phenotype <- sub('.Alter.*', '', drug_phenotype$Phenotype, ignore.case = TRUE)
  drug_phenotype$Phenotype <- sub('.Amino acid sequence.*', '', drug_phenotype$Phenotype, ignore.case = TRUE)
  drug_phenotype$Phenotype <- sub('Warning.*', 'No phenotype information available', drug_phenotype$Phenotype, ignore.case = TRUE)
  drug_phenotype$Phenotype <- str_c(drug_phenotype$Class, ' : ', drug_phenotype$Phenotype)
  
  # Match ST & Phenotype
  df1 = data.frame(data$cenmigID, data$ST)
  colnames(df1) = c('cenmigID', 'ST')
  Pheno_ST = merge(x = drug_phenotype, y = df1, by = "cenmigID", all.x = TRUE)
  
  # Filter unwanted DATA
  Pheno_ST = Pheno_ST %>% drop_na(Phenotype)
  
  # Data count of each ST
  ST_count = Pheno_ST %>% group_by(ST) %>% summarise(count = n())
  colnames(ST_count) = c('ST', 'Count_ST')
  
  # Grouping ST & Phenotype
  Pheno_ST = Pheno_ST %>% group_by(ST,Phenotype) %>% summarise(count = n())
 
  # Merge ST count with Phenotype count 
  Pheno_ST = merge(x = Pheno_ST, y = ST_count, by = "ST", all.x = TRUE)
  Pheno_ST$Percent = 100 * Pheno_ST$count / Pheno_ST$Count_ST
  
  # Export
  name = str_replace(spp, ' ', '_')
  file_csv = paste(file_out, name, '_', file_name, '.csv', sep='')
  write.csv(Pheno_ST, file_csv, row.names = FALSE)
  }

loop_run(meta, resist ,species, file_out)

