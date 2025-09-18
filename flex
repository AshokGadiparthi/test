Got it 👍 You want a **Spring Boot service** that:

1. Creates a **professional PDF report** with a 4-column table.
2. Emails the PDF as an attachment to your team.

We’ll use:

* `iText 7` (or OpenPDF) for PDF generation.
* `Spring Boot Starter Mail` for sending email with attachment.

Here’s a **working example**:

---

### 1. Add Dependencies (`pom.xml`)

```xml
<dependencies>
    <!-- PDF generation with OpenPDF -->
    <dependency>
        <groupId>com.github.librepdf</groupId>
        <artifactId>openpdf</artifactId>
        <version>1.3.39</version>
    </dependency>

    <!-- Spring Boot Mail -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-mail</artifactId>
    </dependency>
</dependencies>
```

---

### 2. `application.properties`

```properties
spring.mail.host=smtp.gmail.com
spring.mail.port=587
spring.mail.username=your_email@gmail.com
spring.mail.password=your_app_password
spring.mail.properties.mail.smtp.auth=true
spring.mail.properties.mail.smtp.starttls.enable=true
```

*(use Gmail app password or SMTP creds for your org)*

---

### 3. PDF Utility (`PdfReportService.java`)

```java
package com.example.demo.service;

import com.lowagie.text.*;
import com.lowagie.text.pdf.PdfPCell;
import com.lowagie.text.pdf.PdfPTable;
import com.lowagie.text.pdf.PdfWriter;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;

@Service
public class PdfReportService {

    public byte[] generateReport() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        Document document = new Document(PageSize.A4);
        PdfWriter.getInstance(document, out);
        document.open();

        // Title
        Font titleFont = new Font(Font.HELVETICA, 16, Font.BOLD);
        Paragraph title = new Paragraph("Team Report", titleFont);
        title.setAlignment(Element.ALIGN_CENTER);
        document.add(title);
        document.add(Chunk.NEWLINE);

        // Table with 4 columns
        PdfPTable table = new PdfPTable(4);
        table.setWidthPercentage(100);
        table.setSpacingBefore(10f);
        table.setSpacingAfter(10f);
        table.setWidths(new float[]{2f, 3f, 3f, 2f});

        // Header row
        String[] headers = {"ID", "Name", "Role", "Status"};
        for (String h : headers) {
            PdfPCell cell = new PdfPCell(new Phrase(h, new Font(Font.HELVETICA, 12, Font.BOLD, Color.WHITE)));
            cell.setBackgroundColor(new Color(63, 81, 181)); // Professional blue
            cell.setHorizontalAlignment(Element.ALIGN_CENTER);
            table.addCell(cell);
        }

        // Sample rows
        table.addCell("1");
        table.addCell("Alice Johnson");
        table.addCell("GCP Engineer");
        table.addCell("Active");

        table.addCell("2");
        table.addCell("Bob Smith");
        table.addCell("Data Engineer");
        table.addCell("Onboarding");

        table.addCell("3");
        table.addCell("Charlie Lee");
        table.addCell("Cloud Architect");
        table.addCell("Active");

        document.add(table);
        document.close();

        return out.toByteArray();
    }
}
```

---

### 4. Email Service (`EmailService.java`)

```java
package com.example.demo.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;

import javax.mail.internet.MimeMessage;

@Service
public class EmailService {

    @Autowired
    private JavaMailSender mailSender;

    public void sendReport(byte[] pdfBytes, String[] recipients) throws Exception {
        MimeMessage message = mailSender.createMimeMessage();
        MimeMessageHelper helper = new MimeMessageHelper(message, true);

        helper.setTo(recipients);
        helper.setSubject("Team Report PDF");
        helper.setText("Hi Team,\n\nPlease find attached the latest team report.\n\nRegards,\nYour System");

        helper.addAttachment("team-report.pdf", new ByteArrayResource(pdfBytes));

        mailSender.send(message);
    }
}
```

---

### 5. Controller (`ReportController.java`)

```java
package com.example.demo.controller;

import com.example.demo.service.EmailService;
import com.example.demo.service.PdfReportService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/report")
public class ReportController {

    @Autowired
    private PdfReportService pdfReportService;

    @Autowired
    private EmailService emailService;

    @PostMapping("/send")
    public String sendReport(@RequestParam String[] to) {
        try {
            byte[] pdf = pdfReportService.generateReport();
            emailService.sendReport(pdf, to);
            return "Report sent successfully!";
        } catch (Exception e) {
            return "Error sending report: " + e.getMessage();
        }
    }
}
```

---

✅ Now, if you hit:

```
POST http://localhost:8080/report/send?to=someone@example.com&to=team@example.com
```

it will:

1. Generate a **professional PDF report with 4 columns**
2. Attach it to the email
3. Send it to the given team members

---

Do you want me to also make the **PDF table dynamic (from DB or API data)** instead of static rows, so it’s fully reusable?
