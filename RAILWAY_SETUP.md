# Railway Deployment Setup Guide

## 🚂 Complete Railway Deployment Instructions

### 1. Create Railway Project

1. Go to [Railway.app](https://railway.app) and sign in
2. Click **"New Project"**
3. Select **"Deploy from GitHub repo"**
4. Choose your repository: `Salamony4all/wrokflow-v2`
5. Railway will automatically start building

### 2. Add Volume for Persistent Brand Database

**Important:** Without a volume, all scraped brand data will be lost when the app redeploys!

#### Create Volume:
1. In your Railway project, click on your service
2. Go to the **"Volumes"** tab
3. Click **"+ New Volume"**
4. Configure the volume:
   - **Mount Path**: `/app/persistent_brands_data`
   - **Size**: Start with 1GB (can increase later)
5. Click **"Add"**

### 3. Configure Environment Variables

Go to the **"Variables"** tab and add these environment variables:

#### Required Variables:

```bash
# Point brands data to the persistent volume
BRANDS_DATA_PATH=/app/persistent_brands_data

# Secret key for Flask sessions (generate a random string)
SECRET_KEY=your-random-secret-key-here-change-this

# Railway provides this automatically - don't add manually
PORT=5000
```

#### Optional API Credentials:

```bash
# PP-Structure API (if you want to change from default)
PP_STRUCTURE_API_URL=https://wfk3ide9lcd0x0k9.aistudio-hub.baidu.com/layout-parsing
PP_STRUCTURE_TOKEN=your-token-here
```

### 4. Generate a Secure Secret Key

Run this command locally to generate a secure secret key:

```bash
python -c "import secrets; print(secrets.token_hex(32))"
```

Copy the output and use it as your `SECRET_KEY` variable.

### 5. Deploy

Railway will automatically deploy after you:
- Add the volume
- Set the environment variables

Watch the deployment logs for any errors.

### 6. Verify Deployment

Once deployed:
1. Click **"Open App"** to view your application
2. Test the scraping functionality
3. Check that new brand data persists after redeployment

## 📁 Directory Structure on Railway

```
/app/                                    # Application root
├── app.py                              # Main application
├── brands_data/                        # Local (non-persistent, in repo)
│   ├── OTTIMO_budgetary.json
│   ├── MARELLI_high_end.json
│   └── brands_dynamic.json
├── persistent_brands_data/             # VOLUME (persistent)
│   ├── brands_dynamic.json            # Master database
│   ├── BRANDNAME_tier.json            # Individual brand files
│   └── [all new scraped data]
├── uploads/                            # Temporary uploads
├── outputs/                            # Generated presentations
└── utils/                              # Utility modules
```

## 🔄 How Brand Data Persistence Works

### Without Volume:
- Scraped data saved to `/app/brands_data/`
- **Lost on every redeploy** 
- ❌ Not recommended for production

### With Volume (Recommended):
- Set `BRANDS_DATA_PATH=/app/persistent_brands_data`
- All scraped data saved to volume
- **Persists across redeploys** ✅
- Data survives updates, restarts, and scaling

## 📊 Volume Management

### Check Volume Usage:
```bash
# In Railway shell/logs
du -sh /app/persistent_brands_data
```

### Backup Brand Data:
```bash
# Download via Railway CLI
railway run tar -czf brands_backup.tar.gz persistent_brands_data
railway run cat brands_backup.tar.gz > local_backup.tar.gz
```

### Increase Volume Size:
1. Go to Railway project → Volumes tab
2. Click on your volume
3. Update size (note: can only increase, not decrease)
4. Redeploy

## 🔍 Troubleshooting

### Issue: Brand data not persisting

**Solution:**
1. Verify volume is mounted: Check Railway → Volumes tab
2. Verify environment variable: `BRANDS_DATA_PATH=/app/persistent_brands_data`
3. Check logs for "Brands data directory:" message
4. Redeploy after adding volume

### Issue: Presentation PDF generation fails

**Solution:**
- This should work automatically with `nixpacks.toml`
- Check deployment logs for LibreOffice installation
- Verify `libreoffice --version` in Railway shell

### Issue: Permission denied errors

**Solution:**
```bash
# Railway volumes should have correct permissions automatically
# If issues persist, check Railway support
```

## 📈 Scaling Considerations

### Single Instance (Default):
- Volume works perfectly
- All scraped data available instantly

### Multiple Instances (Horizontal Scaling):
- ⚠️ Railway volumes are **single-instance**
- For multi-instance: Consider using Railway PostgreSQL or external storage (S3)
- Current setup optimized for single instance

## 🎯 Best Practices

1. **Always use a volume** for production
2. **Set SECRET_KEY** to a random value
3. **Backup brand data** periodically
4. **Monitor volume usage** - brands with many products can use significant space
5. **Use environment variables** for all sensitive data

## 📝 Initial Brand Data

The repository includes sample brand files in `brands_data/`:
- OTTIMO_budgetary.json
- MARELLI_high_end.json
- MARTEX_high_end.json
- etc.

On first deployment with volume:
1. These files exist in `/app/brands_data/` (from repo)
2. New scraped brands go to `/app/persistent_brands_data/` (volume)
3. App checks volume first, then falls back to repo data

## 🔗 Useful Links

- [Railway Volumes Documentation](https://docs.railway.app/reference/volumes)
- [Railway Environment Variables](https://docs.railway.app/develop/variables)
- [GitHub Repository](https://github.com/Salamony4all/wrokflow-v2)

## 🆘 Support

If you encounter issues:
1. Check Railway deployment logs
2. Verify all environment variables are set
3. Ensure volume is properly mounted
4. Check GitHub repo for updates

---

**Last Updated:** December 2025  
**Version:** 2.0
