#!/usr/bin/env python3
"""Restructure index.html with new layout while preserving functionality."""

def main():
    with open('templates/index_backup_original.html', 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Find the script section (starts after forms, before </body>)
    script_start_marker = '  <script>'
    script_start_idx = content.rfind(script_start_marker)
    
    # Find where body starts
    body_start_marker = '<body'
    body_start_idx = content.find(body_start_marker)
    body_content_start = content.find('>', body_start_idx) + 1
    
    # Get head but replace body tag
    head_section = content[:body_start_idx] + '<body>'
    
    # Extract sections
    head_section = content[:body_content_start]
    script_and_closing = content[script_start_idx:]
    
    # Create new body structure
    new_body = '''
  <!-- Sidebar -->
  <div class="sidebar">
    <div class="sidebar-brand">
      Federation
    </div>
    
    <nav class="sidebar-nav">
      <div class="nav-group-title">OPERATIONS</div>
      <a href="{{ url_for('index') }}" class="nav-item active">Onboard PC</a>
      <a href="#" class="nav-item">Onboarded setup details</a>
    </nav>
    
    <div class="sidebar-footer">
      <a href="#">Host details</a>
    </div>
  </div>

  <!-- Main Content -->
  <div class="main-content">
    <!-- Header -->
    <div class="main-header">
      <div class="breadcrumb">Onboard</div>
      <h1 class="page-title">Onboard PC</h1>
    </div>

    <!-- Content Area -->
    <div class="content-area">
      {% if success %}
      <div class="message success">{{ success }}</div>
      {% endif %}
      {% if error %}
      <div class="message error">{{ error }}{% if error_schedule_conflict %} Use "Cancel schedule" for that host first.{% endif %}</div>
      {% endif %}

      <!-- Wrap existing form in form-card -->
      <div class="form-card">
        <!-- Keep all existing form content -->
'''
    
    # Extract the main form content (between body start and script start)
    form_content = content[body_content_start:script_start_idx]
    
    # Remove redundant header elements from old content
    import re
    
    # Remove the old header, subtitle, and duplicate success/error message blocks
    # This pattern matches everything from h1 to the end of the second {% endif %}
    pattern = (
        r'^\s*<h1>Bulk VM snapshots</h1>\s*'
        r'<p class="muted">.*?</p>\s*'
        r'\s*\{% if success %\}.*?\{% endif %\}\s*'
        r'\s*\{% if error %\}.*?\{% endif %\}\s*'
    )
    form_content = re.sub(pattern, '\n', form_content, flags=re.DOTALL | re.MULTILINE, count=1)
    
    # Close the new structure
    new_body_closing = '''
      </div>
    </div>
  </div>
'''
    
    # Combine everything
    new_content = head_section + new_body + form_content + new_body_closing + script_and_closing
    
    with open('templates/index_new2.html', 'w', encoding='utf-8') as f:
        f.write(new_content)
    
    print("Created index_new2.html")

if __name__ == '__main__':
    main()
